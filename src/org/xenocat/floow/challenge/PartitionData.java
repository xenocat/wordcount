package org.xenocat.floow.challenge;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Spliterator;
import java.util.Spliterators.AbstractSpliterator;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * The input file is conceptually split into a number of partitions. Since
 * partition size is a known constant across all agents, a partition can be
 * identified just by its index number, which implies the start and end
 * positions of the partition within the file.
 * <p>
 * This class represents the portion of the input file that is associated with a
 * given partition index.
 * <p>
 * Given the input file, PartitionData provides a Stream of the words in the
 * partition.
 * <p>
 * A word is defined as a sequence of characters that match the regex
 * <code>[\w\p{Alpha}]+</code>, separated by sequences of characters that do
 * not.
 * 
 * @author Ruth Foster
 */
public class PartitionData {
	/**
	 * Partition size is a known constant across all agents.
	 * <p>
	 * There is an implicit assumption that no word is longer than the partition
	 * size (if it were, it could not be read from the partition's word stream).
	 * Given that the partition size is of the order of hundreds of megabytes,
	 * this is probably a pretty good assumption.
	 */
	public static final long PARTITION_SIZE = 0x20000000; // 512 Mb

	/**
	 * Assumed character encoding of the input file.
	 */
	public static final String ENCODING = "utf-8";

	// private static final Pattern NON_WORD_CHAR = Pattern.compile("\\W");
	// private static final Pattern WORD_CHAR = Pattern.compile("\\w");

	private final File file;
	private final int index;
	private final long fileSize;

	/**
	 * @param file
	 *            The data file.
	 * @param index
	 *            The index of this partition data.
	 * 
	 * @throws IndexOutOfBoundsException
	 *             if index is negative, or greater than the maximum partition
	 *             index in the given file.
	 */
	public PartitionData(File file, int index) {
		this.file = file;
		this.index = index;
		this.fileSize = file.length();

		if (index < 0 || index * PARTITION_SIZE >= fileSize) {
			throw new IndexOutOfBoundsException("Partition index " + index + " out of bounds for file " + file
					+ " of length " + fileSize + " with partition size " + PARTITION_SIZE + ".");
		}
	}

	private static Pattern WORD_CHARACTER = Pattern.compile("[\\p{Alpha}\\w-]");
	private static Pattern WORD_CHARACTERS = Pattern.compile("[\\p{Alpha}\\w-]+");
	private static Pattern NON_WORD_CHARACTERS = Pattern.compile("[^\\p{Alpha}\\w-]+");

	private static boolean isWordCharacter(char c) {
		return WORD_CHARACTER.matcher(String.valueOf(c)).matches();
	}

	/**
	 * Returns data starting at the beginning of this partition, with the given
	 * length (or less if the file ends sooner), as a CharBuffer.
	 */
	private CharBuffer getCharBuffer(long byteLength) throws IOException {
		try (RandomAccessFile file = new RandomAccessFile(this.file, "r")) {
			FileChannel channel = file.getChannel();

			long start = index * PARTITION_SIZE;
			long end = Math.min(fileSize, start + byteLength);
			long actualLength = end - start;

			MappedByteBuffer buffer = channel.map(MapMode.READ_ONLY, start, actualLength);

			// TODO: Not happy with this as it copies the entire memory-mapped
			// buffer onto the heap. There's room on the heap to do this, as the
			// partition size is small enough, but there must be a better way to
			// read a MemoryMappedBuffer as a decoded character stream?
			return Charset.forName(ENCODING).newDecoder().onUnmappableCharacter(CodingErrorAction.IGNORE)
					.decode(buffer);
		}
	}

	public Stream<String> getWordStream() throws IOException {
		return getWordStream(index > 0);
	}

	private static final long RUN_ON_MAX_LENGTH = 100;

	private Stream<String> getWordStream(boolean skipFirstWord) throws IOException {
		final CharBuffer charBuffer = getCharBuffer(PARTITION_SIZE);

		final Scanner scanner = new Scanner(charBuffer).useDelimiter(NON_WORD_CHARACTERS);

		// TODO: Currently disabled; this is not right yet.
		// If the first character in the partition is a word character
		// (and this is not the first partition), then this word may be a run-on
		// from the previous partition. Skip it, as the previous partition
		// will deal with it.

		/*if (skipFirstWord && isWordCharacter(charBuffer.charAt(0))) {
			scanner.next();
		}*/

		return StreamSupport.<String>stream(new AbstractSpliterator<String>(Long.MAX_VALUE,
				Spliterator.ORDERED & Spliterator.IMMUTABLE & Spliterator.NONNULL) {
			@Override
			public boolean tryAdvance(Consumer<? super String> consumer) {
				if (scanner.hasNext()) {
					String word = scanner.next();

					// TODO: Currently disabled; this is not right yet.
					// If we are at the end of the partition, and the last
					// character in the partition is a word character (and
					// this is not the last partition), then this word may
					// run on to the next partition. We read word characters
					// from the start of the next partition to complete the
					// word. Only at most RUN_ON_MAX_LENGTH characters are read
					// from the next partition, to avoid loading the entire
					// partition for a single word. (Run-ons longer than this
					// will not be detected.)

					// TODO: If the partition break occurs in the middle of
					// a multi-byte code-point, then we have a problem, because
					// that character will not be read properly, which will
					// affect this logic. This is probably unlikely to happen
					// though as the input is English, and multi-byte
					// code-points should be rare.

					/*if (index < getMaxIndex() && !scanner.hasNext()
							&& isWordCharacter(charBuffer.charAt(charBuffer.length() - 1))) {
						PartitionData nextPartition = new PartitionData(file, index + 1);
					
						try {
							Scanner nextPartitionScanner = new Scanner(nextPartition.getCharBuffer(RUN_ON_MAX_LENGTH));
					
							String runOn = nextPartitionScanner.findWithinHorizon(WORD_CHARACTERS, 0);
					
							if (runOn != null) {
								word = word + runOn;
							}
						} catch (IOException e) {
							throw new IllegalStateException("Unable to read next partition.", e);
						}
					}*/

					consumer.accept(word);

					return true;
				} else {
					return false;
				}
			}
		}, false);
	}

	public int getIndex() {
		return index;
	}

	/**
	 * The highest possible partition index for the given file size.
	 */
	private int getMaxIndex() {
		return (int) (fileSize / PARTITION_SIZE) + (fileSize % PARTITION_SIZE > 0 ? 1 : 0);
	}

	/**
	 * Counts and returns all the unique words in this partition's word stream
	 * as a map of Strings to Longs. All words are converted to lower case
	 * before being counted.
	 * <p>
	 * This operation is interruptible and will throw InterruptedException if
	 * the current thread is interrupted while counting a long word stream.
	 * 
	 * @throws InterruptedException
	 *             if the current thread is interrupted while counting.
	 */
	public Map<String, Long> getWordCounts() throws IOException, InterruptedException {
		// We could just write the below, except we need this operation to be
		// interruptible, so we must loop and count manually.

		// return getWordStream().map((String s) -> new
		// CaseInsensitiveString(s)).collect(Collectors.groupingBy(Function.identity(),
		// Collectors.counting()));

		Map<String, Long> counts = new LinkedHashMap<>();
		Thread currentThread = Thread.currentThread();

		for (String word : (Iterable<String>) getWordStream()::iterator) {
			if (currentThread.isInterrupted()) {
				throw new InterruptedException("Interrupted while counting.");
			}
			
			word = word.toLowerCase();

			Long count = counts.get(word);

			if (count == null) {
				count = 0L;
			}

			count++;

			counts.put(word, count);
		}

		return counts;
	}

}
