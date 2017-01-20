package org.xenocat.floow.challenge;

import java.io.File;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

/**
 * Parses the command line arguments into an options bean. Validates that
 * -source, -id must be specified if either one of them is, and that they must
 * be specified unless either of -report or -cleardown are. The -mongo option is
 * always required.
 * <p>
 * The existence of the file specified by the -source option (if present) is
 * also validated.
 * 
 * @author Ruth Foster
 */
public class Options {
	public static final String SOURCE_OPTION = "-source";
	public static final String MONGO_OPTION = "-mongo";
	public static final String ID_OPTION = "-id";
	public static final String CLEARDOWN_OPTION = "-cleardown";
	public static final String REPORT_OPTION = "-report";

	private static final Set<String> ALLOWED_OPTIONS = new LinkedHashSet<>(
			Arrays.asList(SOURCE_OPTION, MONGO_OPTION, ID_OPTION, CLEARDOWN_OPTION, REPORT_OPTION));

	private final String fileName;
	private final String mongoHost;
	private final String agentId;
	private final String report;
	private final boolean cleardown;

	public Options(String... args) {
		this(toOptionMap(ALLOWED_OPTIONS, args));
	}

	private Options(Map<String, String> args) {
		this.fileName = args.get(SOURCE_OPTION);
		this.mongoHost = args.get(MONGO_OPTION);
		this.agentId = args.get(ID_OPTION);
		this.report = args.get(REPORT_OPTION);
		this.cleardown = args.containsKey(CLEARDOWN_OPTION);

		// Mongo host is always mandatory.
		Objects.requireNonNull(mongoHost, "MongoDB host must be specified with " + MONGO_OPTION + " option.");

		// At least one of report, cleardown or id must be specified.
		if (agentId == null && report == null && !cleardown) {
			throw new IllegalArgumentException("At least one of " + ID_OPTION + ", " + REPORT_OPTION + " or "
					+ CLEARDOWN_OPTION + " must be specified.");
		}

		// If agent id is specified, source file must be also.
		if (agentId != null) {
			Objects.requireNonNull(fileName, "Source file path must be specified with " + SOURCE_OPTION + " option.");
		}

		// If a source file is specified, it must exist.
		if (fileName != null && !getFile().exists()) {
			throw new IllegalArgumentException("Source file " + fileName + " not found.");
		}
	}

	/**
	 * Transforms an array of command-line arguments into a map of option names
	 * to values. The arguments are presumed to be presented in the order
	 * option, value, option, value etc. If there are an odd number of
	 * arguments, the final option has the value null.
	 * 
	 * @param allowedOptions
	 *            If non-null, this specifies a set of allowed options. Any
	 *            option found not in this set will cause
	 *            IllegalArgumentException to be thrown.
	 * @param args
	 *            The array of command-line arguments.
	 * @throws NullPointerException
	 *             if args is null.
	 * @throws IllegalArgumentException
	 *             if allowedOptions is non-null and an option not in this set
	 *             is found in args.
	 */
	private static final Map<String, String> toOptionMap(Set<String> allowedOptions, String... args) {
		Map<String, String> map = new LinkedHashMap<>(args.length / 2);

		for (int i = 0; i < args.length; i += 2) {
			String option = args[i];
			String value = i < args.length - 1 ? args[i + 1] : null;

			if (allowedOptions != null && !allowedOptions.contains(option)) {
				throw new IllegalArgumentException("Unexpected command-line option: " + option + ".");
			}

			map.put(option, value);
		}

		return map;
	}

	public String getFileName() {
		return fileName;
	}

	public String getMongoHost() {
		return mongoHost;
	}

	public String getAgentId() {
		return agentId;
	}

	public String getReport() {
		return report;
	}

	public boolean isCleardown() {
		return cleardown;
	}

	public MongoClient getMongoClient() {
		String mongoHost = getMongoHost();

		if (mongoHost == null) {
			throw new IllegalStateException("MongoDB host not specified.");
		}

		return new MongoClient(new MongoClientURI("mongodb://" + mongoHost));
	}

	public File getFile() {
		String fileName = getFileName();

		return fileName != null ? new File(fileName) : null;
	}

	public File getReportFile() {
		String report = getReport();

		return report != null ? new File(report) : null;
	}
}
