package org.xenocat.floow.challenge;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;

/**
 * @author Ruth Foster
 */
public class Agent {
	/**
	 * A running agent will update the heartbeat field of a partition that it is
	 * working on approximately every this number of milliseconds, to indicate
	 * to other agents that it is still alive.
	 */
	public static final long HEART_BEAT_INTERVAL = 5000;

	/**
	 * If an idle agent notices that another agent that claims to be working on
	 * a partition has not updated its heartbeat field in this number of
	 * milliseconds, it may assume that the non-updating agent is dead and may
	 * take over working on the partition in question.
	 */
	public static final long DEATH_INTERVAL = HEART_BEAT_INTERVAL * 2;

	private final Options options;

	/**
	 * The number of partitions that make up the input file. This is computed
	 * from the partition size and the file length.
	 */
	private final int partitionCount;

	private MongoClient mongoClient;
	private ScheduledExecutorService heartbeatExecutor;
	private ExecutorService mainLoopExecutor;
	private boolean started;

	/**
	 * The partition currently being worked on, if any.
	 */
	private Partition assignedPartition;

	/**
	 * A reference to the main thread, while it is running. This is used by the
	 * heartbeat thread to interrupt it, if necessary.
	 */
	private Thread mainThread;

	public Agent(Options options) {
		this.options = Objects.requireNonNull(options);

		long fileSize = options.getFile().length();

		partitionCount = (int) (fileSize / PartitionData.PARTITION_SIZE)
				+ (fileSize % PartitionData.PARTITION_SIZE > 0 ? 1 : 0);

		// Shut down this agent (if it is not already) upon JVM termination.
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				shutdown();
			}
		});
	}

	public String getId() {
		return getOptions().getAgentId();
	}

	public Options getOptions() {
		return options;
	}

	public synchronized boolean isStarted() {
		return started;
	}

	/**
	 * Starts this agent running. This opens the MongoDB connection, and begins
	 * two threads:
	 * <ul>
	 * <li>The main loop thread, which searches for an available partition to
	 * work on, counts the words in it when it finds one, and updates the
	 * database with the words that it counted.</li>
	 * <li>The heartbeat thread, which periodically updates the heartbeat field
	 * for the partition currently being worked on, to prove to other agents
	 * that this agent is still alive. The heartbeat also checks to see if this
	 * agent is still the owner of the partition it is working on. If another
	 * agent claims the partition that this agent was in the middle of working
	 * on, then the main thread will be interrupted and will search for an
	 * alternative partition instead.</li>
	 * </ul>
	 * This method is idempotent. If the agent is already started, the call has
	 * no effect.
	 */
	public synchronized void start() {
		if (isStarted()) {
			return;
		}

		log("Starting up.");

		mongoClient = getOptions().getMongoClient();

		mainLoopExecutor = Executors.newSingleThreadExecutor();
		mainLoopExecutor.submit(new Runnable() {
			public void run() {
				mainLoop();
			}
		}, null);

		heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
		heartbeatExecutor.scheduleAtFixedRate(new Runnable() {
			public void run() {
				heartbeat();
			}
		}, HEART_BEAT_INTERVAL, HEART_BEAT_INTERVAL, TimeUnit.MILLISECONDS);

		// Ensure required indexes exist in the database
		createIndexes();

		started = true;
	}

	/**
	 * Stops the agent running. The MongoDB connection is closed, and the main
	 * and heartbeat threads are interrupted and will terminate.
	 * <p>
	 * This method is idempotent. If the agent is not currently running, the
	 * call has no effect.
	 */
	public synchronized void shutdown() {
		if (!isStarted()) {
			return;
		}

		log("Shutting down.");

		mainLoopExecutor.shutdownNow();
		heartbeatExecutor.shutdownNow();

		mongoClient.close();

		started = false;
	}

	private void mainLoop() {
		synchronized (this) {
			mainThread = Thread.currentThread();
		}

		try {
			while (isStarted()) {
				try {
					// Find a partition to work on.
					log("Looking for a partition to work on.");

					Partition availablePartition = findAvailablePartition();

					if (availablePartition == null) {
						// No partitions are available to work on.
						log("No partitions available.");

						if (isJobDone()) {
							// If all partitions are completed, the task is done
							// and this agent can shut down.
							log("All partitions completed.");

							return;
						} else {
							// Otherwise, at least one partition is not yet
							// marked as completed. Even though it is not
							// currently available, it may become available in
							// future if the agent working on it dies. This
							// agent will now sleep for a heartbeat before
							// checking again for available partitions, so that
							// we are not querying the database in a tight loop.
							try {
								Thread.sleep(HEART_BEAT_INTERVAL);
							} catch (InterruptedException e) {
								log("Interrupted while waiting for an available partition.");
							}
						}
					} else {
						try {
							// Found a partition to work on. Claim it.
							if (availablePartition.getOwner() != null) {
								// If the available partition has a previous
								// owner, it has become available because that
								// owner has not updated the heartbeat recently
								// enough. We assume the previous owner has
								// died, and take over the partition.
								log("Claiming " + availablePartition + " previously owned by agent "
										+ availablePartition.getOwner() + ".");
							} else {
								log("Claiming " + availablePartition + ".");
							}

							availablePartition.claim().save();

							// Count the words in the partition's word stream
							// and update the database. This operation is
							// interruptible. The current thread may be
							// interrupted by this Agent's own heartbeat if it
							// discovers that we have lost the partition to
							// another Agent.
							processPartition(availablePartition);
						} catch (InterruptedException e) {
							log("Interrupted while working on partition " + availablePartition.getIndex() + ".");
						}
					}
				} catch (Exception e) {
					log("Exception in main loop.", e);
				}
			}
		} catch (Throwable e) {
			log("Error in main loop.", e);
		} finally {

			shutdown();

			synchronized (this) {
				mainThread = null;
			}
		}
	}

	/**
	 * Claims the specified partition and begins work on it.
	 * 
	 * <p>
	 * This method cannot be called concurrently or reentrantly, and will throw
	 * IllegalStateException if the agent is already working on a partition.
	 * <p>
	 * This operation is interruptible.
	 * 
	 * @param Partition
	 *            the partition to work on.
	 * @throws IllegalStateException
	 *             If this Agent is already working on a partition.
	 * @throws InterruptedException
	 *             if the current thread is interrupted while counting words in
	 *             the partition.
	 */
	private void processPartition(Partition partition) throws InterruptedException {
		synchronized (this) {
			if (assignedPartition != null) {
				throw new IllegalStateException(
						"Cannot work on " + partition + " because this agent is currently working on "
								+ assignedPartition + ". Only one partition may be processed at once by each agent.");
			}

			assignedPartition = new Partition(partition.getIndex());
		}

		try {
			log("Counting words in " + partition + ".");

			Map<String, Long> wordCounts = partition.getPartitionData().getWordCounts();

			// After counting words, check that we are still the owner of the
			// assigned partition before beginning to update the counts in the
			// database and then finally flagging the partition as completed.
			if (!partition.load().isOwned()) {
				log("Lost ownership of partition " + partition.getIndex()
						+ " while working. Abandoning it and looking for a new partition.");

				return;
			}

			log(wordCounts.size() + " unique words discovered in " + partition + ".");
			log("Updating word counts for " + partition + ".");

			incrementCounts(wordCounts);

			partition.complete().save();

			log(partition + " completed.");
		} catch (IOException e) {
			log("Unable to read data for " + partition + ". Agent will terminate.", e);

			shutdown();
		} finally {
			synchronized (this) {
				assignedPartition = null;
			}
		}
	}

	/**
	 * Bulk writes sent by {@link #incrementCounts(Map)} will contain at most
	 * this number of updates. The complete set of updates will be split across
	 * several batches in this way.
	 */
	private static final int INCREMENT_BATCH_SIZE = 100000;

	/**
	 * Updates the counts collection in the database, incrementing the count
	 * field for each word.
	 * <p>
	 * This operation is interruptible.
	 * 
	 * @throws InterruptedException
	 *             if the current thread is interrupted while building the
	 *             update list.
	 */
	private void incrementCounts(Map<String, Long> wordCounts) throws InterruptedException {
		Thread currentThread = Thread.currentThread();

		// Could write the following, but need to loop manually to make it
		// interruptible.
		/*List<UpdateOneModel<Document>> updates = wordCounts.entrySet().stream().map((Entry<String, Long> entry) -> {
			return new UpdateOneModel<Document>(Filters.eq(WORD_FIELD, entry.getKey()),
					Updates.combine(Updates.setOnInsert(WORD_FIELD, entry.getKey()),
							Updates.inc(COUNT_FIELD, entry.getValue())),
					new UpdateOptions().upsert(true));
		}).collect(Collectors.toList());*/

		List<UpdateOneModel<Document>> updates = new ArrayList<>(Math.min(wordCounts.size(), INCREMENT_BATCH_SIZE));

		UpdateOptions updateOptions = new UpdateOptions().upsert(true);

		for (Entry<String, Long> entry : wordCounts.entrySet()) {
			if (currentThread.isInterrupted()) {
				throw new InterruptedException("Interrupted while updating counts.");		
			}
			
			updates.add(new UpdateOneModel<Document>(Filters.eq(WORD_FIELD, entry.getKey()),
					Updates.combine(Updates.setOnInsert(WORD_FIELD, entry.getKey()),
							Updates.inc(COUNT_FIELD, entry.getValue())),
					updateOptions));

			// Send a bulkWrite request every INCREMENT_BATCH_SIZE entries, so
			// that we are not sending requests that are too big for the
			// connection timeout.
			if (updates.size() % INCREMENT_BATCH_SIZE == 0) {

				getCounts().bulkWrite(updates, new BulkWriteOptions().ordered(false));

				log("Batch of " + updates.size() + " updates sent.");

				updates = new ArrayList<>(Math.min(wordCounts.size(), INCREMENT_BATCH_SIZE));
			}
		}

		// Don't need acknowledgement for this update.
		getCounts().bulkWrite(updates, new BulkWriteOptions().ordered(false));
	}

	private synchronized void heartbeat() {
		try {
			int assignedIndex;

			synchronized (this) {
				if (assignedPartition == null) {
					// Not currently working on a partition, so the heartbeat
					// has no effect.
					return;
				}

				assignedIndex = assignedPartition.getIndex();
			}

			Partition partition = getPartition(assignedIndex);

			if (!partition.isOwned()) {
				// If we have lost ownership of this partition then abandon work
				// on it. The main thread is interrupted, stopping any word
				// count in progress.
				log("Lost ownership of " + partition + ". Work will be interrupted.");

				partition = null;

				interruptMain();
			} else {
				// If we still have ownership of the partition, then update its
				// heartbeat to prove to other agents that we are still here and
				// working.
				partition.heartbeat().save();

				log(partition + " heartbeat updated at " + new Date(partition.getHeartbeat()) + ".");

			}
		} catch (Exception e) {
			log("Exception in heartbeat.", e);
		}
	}

	/**
	 * Interrupts the main loop thread, if it is running.
	 */
	private synchronized void interruptMain() {
		if (mainThread != null) {
			mainThread.interrupt();
		}
	}

	private Partition getPartition(int partitionIndex) {
		return new Partition(partitionIndex).load();
	}

	private Partition findAvailablePartition() {
		// A partition whose heartbeat is older than the death interval is
		// assumed to have been abandoned by its owner Agent and is available to
		// be claimed by another Agent.
		long deadHeartbeat = System.currentTimeMillis() - DEATH_INTERVAL;

		MongoCollection<Document> partitions = getPartitions();

		// Find an available partition: That is, an uncompleted partition that
		// is either unowned by any agent, or is owned by an agent other than
		// this one, but its heartbeat is older than the death interval.
		FindIterable<Document> availablePartitions = partitions
				.find(Filters.and(Filters.ne(COMPLETED_FIELD, true), Filters.or(Filters.eq(OWNER_FIELD, null),
						Filters.and(Filters.ne(OWNER_FIELD, getId()), Filters.lte(HEARTBEAT_FIELD, deadHeartbeat)))));

		Document available = availablePartitions.first();

		if (available != null) {
			return new Partition(available);
		}

		// If no such partition exists, find an unused partition index (if any),
		// and create a record for it:
		FindIterable<Document> existingPartitions = partitions.find();
		Set<Integer> existingIndexes = StreamSupport
				.stream(existingPartitions.map(d -> d.getInteger(INDEX_FIELD)).spliterator(), false)
				.collect(Collectors.toSet());

		for (int p = 0; p < partitionCount; p++) {
			if (!existingIndexes.contains(p)) {
				return new Partition(p);
			}
		}

		return null;
	}

	/**
	 * Returns true if and only if the entire task is done, that is, if there
	 * are partitionCount partitions existing in the database, and all of them
	 * are flagged as completed.
	 */
	private boolean isJobDone() {
		return getPartitions().count(Filters.eq(COMPLETED_FIELD, true)) >= partitionCount;
	}

	/**
	 * 
	 */
	private static final String DATABASE = "wordCount";

	/**
	 *
	 */
	private static final String PARTITIONS_COLLECTION = "partitions";

	private static final String INDEX_FIELD = "index";
	private static final String OWNER_FIELD = "owner";
	private static final String HEARTBEAT_FIELD = "heartbeat";
	private static final String COMPLETED_FIELD = "completed";

	private static final String COUNTS_COLLECTION = "counts";

	private static final String WORD_FIELD = "word";
	private static final String COUNT_FIELD = "count";

	private MongoClient getMongoClient() {
		return mongoClient;
	}

	private MongoDatabase getDatabase() {
		return getMongoClient().getDatabase(DATABASE);
	}

	private MongoCollection<Document> getPartitions() {
		return getDatabase().getCollection(PARTITIONS_COLLECTION);
	}

	private MongoCollection<Document> getCounts() {
		return getDatabase().getCollection(COUNTS_COLLECTION);
	}

	/**
	 * Ensures the necessary indexes exist in the database.
	 */
	private void createIndexes() {
		MongoCollection<Document> partitions = getPartitions();

		partitions.createIndex(Indexes.ascending(INDEX_FIELD));
		partitions.createIndex(Indexes.hashed(OWNER_FIELD));

		MongoCollection<Document> counts = getCounts();

		counts.createIndex(Indexes.hashed(WORD_FIELD));
		counts.createIndex(Indexes.descending(COUNT_FIELD));
	}

	private class Partition {
		private final int index;

		private String owner;
		private long heartbeat;
		private boolean completed;

		public Partition(int index) {
			if (index < 0 || index >= partitionCount) {
				throw new IndexOutOfBoundsException("Partition index must be from 0 to " + (partitionCount - 1) + ".");
			}

			this.index = index;
		}

		public Partition(Document document) {
			this(document.getInteger(INDEX_FIELD));

			apply(document);
		}

		public int getIndex() {
			return index;
		}

		public String getOwner() {
			return owner;
		}

		private void setOwner(String owner) {
			this.owner = owner;
		}

		public long getHeartbeat() {
			return heartbeat;
		}

		private void setHeartbeat(long heartbeat) {
			this.heartbeat = heartbeat;
		}

		public boolean isCompleted() {
			return completed;
		}

		private void setCompleted(boolean completed) {
			this.completed = completed;
		}

		public PartitionData getPartitionData() {
			return new PartitionData(getOptions().getFile(), getIndex());
		}

		/**
		 * Returns true if and only if this partition is owned by this agent.
		 */
		public boolean isOwned() {
			return getId().equals(getOwner());
		}

		/**
		 * Loads this PartitionStatus's properties from the database, if a
		 * record for it exists. If not, its properties are unchanged.
		 * 
		 * @return This Partition, for call chaining.
		 */
		public Partition load() {
			getPartitions().find(Filters.eq(INDEX_FIELD, getIndex())).forEach((Consumer<Document>) this::apply);

			return this;
		}

		private void apply(Document document) {
			setOwner(document.getString(OWNER_FIELD));
			setCompleted(document.getBoolean(COMPLETED_FIELD));
			setHeartbeat(document.getLong(HEARTBEAT_FIELD));
		}

		/**
		 * Sets the owner of this partition to this agent. Also updates the
		 * heartbeat.
		 * 
		 * @return This Partition, for call chaining.
		 */
		public Partition claim() {
			// getPartitions().withWriteConcern(WriteConcern.ACKNOWLEDGED).
			setOwner(getId());

			return heartbeat();
		}

		/**
		 * Updates the heartbeat property to the current system time.
		 * 
		 * @return This Partition, for call chaining.
		 */
		public Partition heartbeat() {
			setHeartbeat(System.currentTimeMillis());

			return this;
		}

		/**
		 * Sets the completed property to true.
		 * 
		 * @return This Partition, for call chaining.
		 */
		public Partition complete() {
			setCompleted(true);

			return this;
		}

		/**
		 * Writes this Partition's properties to the database.
		 */
		public Partition save() {
			Document document = new Document(INDEX_FIELD, getIndex()).append(OWNER_FIELD, getOwner())
					.append(HEARTBEAT_FIELD, getHeartbeat()).append(COMPLETED_FIELD, isCompleted());

			getPartitions().withWriteConcern(WriteConcern.ACKNOWLEDGED).replaceOne(Filters.eq(INDEX_FIELD, getIndex()),
					document, new UpdateOptions().upsert(true));

			return this;
		}

		public String toString() {
			return "Partition " + getIndex() + " of " + partitionCount;
		}
	}

	public String toString() {
		return "Agent " + getId();
	}

	/**
	 * Logs the given message to System.out, with the agent's id prepended.
	 * 
	 * @param message
	 *            A message to log.
	 */
	private void log(String message) {
		log(message, null);
	}

	/**
	 * Logs the given message to System.out, with the agent's id prepended, and
	 * also prints the given throwable's stack trace.
	 * 
	 * @param message
	 *            A message to log.
	 * @param throwable
	 *            A Throwable. If non-null, the stack trace will be printed to
	 *            System.out, after the message.
	 */
	private void log(String message, Throwable throwable) {
		System.out.println("[" + this + "] " + message);

		if (throwable != null) {
			throwable.printStackTrace(System.out);
		}
	}

	/**
	 * Reports a CSV list of word counts from the database, in descending order
	 * of frequency, to the specified report file, and also to the console.
	 */
	private static void report(Options options) {
		File reportFile = options.getReportFile();

		if (reportFile.getParentFile() != null) {
			reportFile.getParentFile().mkdirs();
		}

		System.out.println("Reporting to file " + reportFile + "...");

		try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(reportFile)))) {
			MongoCollection<Document> counts = options.getMongoClient().getDatabase(DATABASE)
					.getCollection(COUNTS_COLLECTION);

			counts.find().sort(Sorts.descending(COUNT_FIELD)).forEach((Consumer<Document>) (Document d) -> {
				String line = d.getString(WORD_FIELD) + ", " + d.getLong(COUNT_FIELD);

				writer.println(line);
				// System.out.println(line);
			});

			System.out.println("Report complete.");
		} catch (Exception e) {
			System.err.println("Unable to report to file " + options.getReportFile() + ".");
			e.printStackTrace();
		}
	}

	/**
	 * Clears down the data by dropping the entire wordCounts database.
	 */
	private static void cleardown(Options options) {
		System.out.println("Dropping database " + DATABASE + ".");

		options.getMongoClient().getDatabase(DATABASE).withWriteConcern(WriteConcern.ACKNOWLEDGED).drop();
	}

	static {
		// Enable MongoDB logging in general
		System.setProperty("DEBUG.MONGO", "true");

		// Enable DB operation tracing
		System.setProperty("DB.TRACE", "true");
	}

	/**
	 * 
	 * @throws IllegalArgumentException
	 *             if the command-line parameters are not valid (see
	 *             {@link Options}).
	 */
	public static void main(String... args) {
		Options options = new Options(args);

		if (options.getReport() != null) {
			report(options);
		}

		if (options.isCleardown()) {
			cleardown(options);
		}

		if (options.getAgentId() != null) {
			new Agent(options).start();
		}
	}
}
