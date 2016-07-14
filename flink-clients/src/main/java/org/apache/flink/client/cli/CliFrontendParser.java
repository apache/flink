/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.client.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.client.CliFrontend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A simple command line parser (based on Apache Commons CLI) that extracts command
 * line options.
 */
public class CliFrontendParser {

	private static final Logger LOG = LoggerFactory.getLogger(CliFrontendParser.class);


	static final Option HELP_OPTION = new Option("h", "help", false,
			"Show the help message for the CLI Frontend or the action.");

	static final Option JAR_OPTION = new Option("j", "jarfile", true, "Flink program JAR file.");

	static final Option CLASS_OPTION = new Option("c", "class", true,
			"Class with the program entry point (\"main\" method or \"getPlan()\" method. Only needed if the " +
			"JAR file does not specify the class in its manifest.");

	static final Option CLASSPATH_OPTION = new Option("C", "classpath", true, "Adds a URL to each user code " +
			"classloader  on all nodes in the cluster. The paths must specify a protocol (e.g. file://) and be " +
					"accessible on all nodes (e.g. by means of a NFS share). You can use this option multiple " +
					"times for specifying more than one URL. The protocol must be supported by the " +
					"{@link java.net.URLClassLoader}.");

	public static final Option PARALLELISM_OPTION = new Option("p", "parallelism", true,
			"The parallelism with which to run the program. Optional flag to override the default value " +
			"specified in the configuration.");

	static final Option LOGGING_OPTION = new Option("q", "sysoutLogging", false, "If present, " +
			"suppress logging output to standard out.");

	public static final Option DETACHED_OPTION = new Option("d", "detached", false, "If present, runs " +
			"the job in detached mode");

	static final Option ARGS_OPTION = new Option("a", "arguments", true,
			"Program arguments. Arguments can also be added without -a, simply as trailing parameters.");

	public static final Option ADDRESS_OPTION = new Option("m", "jobmanager", true,
			"Address of the JobManager (master) to which to connect. " +
			"Use this flag to connect to a different JobManager than the one specified in the configuration.");

	static final Option SAVEPOINT_PATH_OPTION = new Option("s", "fromSavepoint", true,
			"Path to a savepoint to reset the job back to (for example file:///flink/savepoint-1537).");

	static final Option SAVEPOINT_DISPOSE_OPTION = new Option("d", "dispose", true,
			"Path of savepoint to dispose.");

	// list specific options
	static final Option RUNNING_OPTION = new Option("r", "running", false,
			"Show only running programs and their JobIDs");

	static final Option SCHEDULED_OPTION = new Option("s", "scheduled", false,
			"Show only scheduled programs and their JobIDs");

	static final Option ZOOKEEPER_NAMESPACE_OPTION = new Option("z", "zookeeperNamespace", true,
			"Namespace to create the Zookeeper sub-paths for high availability mode");

	static {
		HELP_OPTION.setRequired(false);

		JAR_OPTION.setRequired(false);
		JAR_OPTION.setArgName("jarfile");

		CLASS_OPTION.setRequired(false);
		CLASS_OPTION.setArgName("classname");

		CLASSPATH_OPTION.setRequired(false);
		CLASSPATH_OPTION.setArgName("url");

		ADDRESS_OPTION.setRequired(false);
		ADDRESS_OPTION.setArgName("host:port");

		PARALLELISM_OPTION.setRequired(false);
		PARALLELISM_OPTION.setArgName("parallelism");

		LOGGING_OPTION.setRequired(false);
		DETACHED_OPTION.setRequired(false);

		ARGS_OPTION.setRequired(false);
		ARGS_OPTION.setArgName("programArgs");
		ARGS_OPTION.setArgs(Option.UNLIMITED_VALUES);

		RUNNING_OPTION.setRequired(false);
		SCHEDULED_OPTION.setRequired(false);

		SAVEPOINT_PATH_OPTION.setRequired(false);
		SAVEPOINT_PATH_OPTION.setArgName("savepointPath");

		ZOOKEEPER_NAMESPACE_OPTION.setRequired(false);
		ZOOKEEPER_NAMESPACE_OPTION.setArgName("zookeeperNamespace");
	}

	private static final Options RUN_OPTIONS = getRunOptions(buildGeneralOptions(new Options()));
	private static final Options INFO_OPTIONS = getInfoOptions(buildGeneralOptions(new Options()));
	private static final Options LIST_OPTIONS = getListOptions(buildGeneralOptions(new Options()));
	private static final Options CANCEL_OPTIONS = getCancelOptions(buildGeneralOptions(new Options()));
	private static final Options STOP_OPTIONS = getStopOptions(buildGeneralOptions(new Options()));
	private static final Options SAVEPOINT_OPTIONS = getSavepointOptions(buildGeneralOptions(new Options()));

	private static Options buildGeneralOptions(Options options) {
		options.addOption(HELP_OPTION);
		// backwards compatibility: ignore verbose flag (-v)
		options.addOption(new Option("v", "verbose", false, "This option is deprecated."));
		// add general options of all CLIs
		for (CustomCommandLine customCLI : CliFrontend.getCustomCommandLineList()) {
			customCLI.addGeneralOptions(options);
		}
		return options;
	}

	public static Options getProgramSpecificOptions(Options options) {
		options.addOption(JAR_OPTION);
		options.addOption(CLASS_OPTION);
		options.addOption(CLASSPATH_OPTION);
		options.addOption(PARALLELISM_OPTION);
		options.addOption(ARGS_OPTION);
		options.addOption(LOGGING_OPTION);
		options.addOption(DETACHED_OPTION);
		options.addOption(SAVEPOINT_PATH_OPTION);
		options.addOption(ZOOKEEPER_NAMESPACE_OPTION);
		return options;
	}

	private static Options getProgramSpecificOptionsWithoutDeprecatedOptions(Options options) {
		options.addOption(CLASS_OPTION);
		options.addOption(CLASSPATH_OPTION);
		options.addOption(PARALLELISM_OPTION);
		options.addOption(LOGGING_OPTION);
		options.addOption(DETACHED_OPTION);
		options.addOption(SAVEPOINT_PATH_OPTION);
		options.addOption(ZOOKEEPER_NAMESPACE_OPTION);
		return options;
	}

	private static Options getRunOptions(Options options) {
		options = getProgramSpecificOptions(options);
		options = getJobManagerAddressOption(options);
		return addCustomCliOptions(options, true);
	}


	private static Options getJobManagerAddressOption(Options options) {
		options.addOption(ADDRESS_OPTION);
		return options;
	}

	private static Options getInfoOptions(Options options) {
		options = getProgramSpecificOptions(options);
		options = getJobManagerAddressOption(options);
		return addCustomCliOptions(options, false);
	}

	private static Options getListOptions(Options options) {
		options.addOption(RUNNING_OPTION);
		options.addOption(SCHEDULED_OPTION);
		options = getJobManagerAddressOption(options);
		return addCustomCliOptions(options, false);
	}

	private static Options getCancelOptions(Options options) {
		options = getJobManagerAddressOption(options);
		return addCustomCliOptions(options, false);
	}

	private static Options getStopOptions(Options options) {
		options = getJobManagerAddressOption(options);
		return addCustomCliOptions(options, false);
	}

	private static Options getSavepointOptions(Options options) {
		options = getJobManagerAddressOption(options);
		options.addOption(SAVEPOINT_DISPOSE_OPTION);
		options.addOption(JAR_OPTION);
		return addCustomCliOptions(options, false);
	}

	// --------------------------------------------------------------------------------------------
	//  Help
	// --------------------------------------------------------------------------------------------

	private static Options getRunOptionsWithoutDeprecatedOptions(Options options) {
		Options o = getProgramSpecificOptionsWithoutDeprecatedOptions(options);
		return getJobManagerAddressOption(o);
	}


	private static Options getInfoOptionsWithoutDeprecatedOptions(Options options) {
		options.addOption(CLASS_OPTION);
		options.addOption(PARALLELISM_OPTION);
		return options;
	}

	private static Options getListOptionsWithoutDeprecatedOptions(Options options) {
		options.addOption(RUNNING_OPTION);
		options.addOption(SCHEDULED_OPTION);
		options = getJobManagerAddressOption(options);
		return options;
	}

	private static Options getCancelOptionsWithoutDeprecatedOptions(Options options) {
		options = getJobManagerAddressOption(options);
		return options;
	}

	private static Options getStopOptionsWithoutDeprecatedOptions(Options options) {
		options = getJobManagerAddressOption(options);
		return options;
	}

	private static Options getSavepointOptionsWithoutDeprecatedOptions(Options options) {
		options = getJobManagerAddressOption(options);
		options.addOption(SAVEPOINT_DISPOSE_OPTION);
		options.addOption(JAR_OPTION);
		return options;
	}

	/**
	 * Prints the help for the client.
	 */
	public static void printHelp() {
		System.out.println("./flink <ACTION> [OPTIONS] [ARGUMENTS]");
		System.out.println();
		System.out.println("The following actions are available:");

		printHelpForRun();
		printHelpForInfo();
		printHelpForList();
		printHelpForStop();
		printHelpForCancel();
		printHelpForSavepoint();

		System.out.println();
	}

	public static void printHelpForRun() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setLeftPadding(5);
		formatter.setWidth(80);

		System.out.println("\nAction \"run\" compiles and runs a program.");
		System.out.println("\n  Syntax: run [OPTIONS] <jar-file> <arguments>");
		formatter.setSyntaxPrefix("  \"run\" action options:");
		formatter.printHelp(" ", getRunOptionsWithoutDeprecatedOptions(new Options()));

		printCustomCliOptions(formatter, true);

		System.out.println();
	}

	public static void printHelpForInfo() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setLeftPadding(5);
		formatter.setWidth(80);

		System.out.println("\nAction \"info\" shows the optimized execution plan of the program (JSON).");
		System.out.println("\n  Syntax: info [OPTIONS] <jar-file> <arguments>");
		formatter.setSyntaxPrefix("  \"info\" action options:");
		formatter.printHelp(" ", getInfoOptionsWithoutDeprecatedOptions(new Options()));

		printCustomCliOptions(formatter, false);

		System.out.println();
	}

	public static void printHelpForList() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setLeftPadding(5);
		formatter.setWidth(80);

		System.out.println("\nAction \"list\" lists running and scheduled programs.");
		System.out.println("\n  Syntax: list [OPTIONS]");
		formatter.setSyntaxPrefix("  \"list\" action options:");
		formatter.printHelp(" ", getListOptionsWithoutDeprecatedOptions(new Options()));

		printCustomCliOptions(formatter, false);

		System.out.println();
	}

	public static void printHelpForStop() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setLeftPadding(5);
		formatter.setWidth(80);

		System.out.println("\nAction \"stop\" stops a running program (streaming jobs only).");
		System.out.println("\n  Syntax: stop [OPTIONS] <Job ID>");
		formatter.setSyntaxPrefix("  \"stop\" action options:");
		formatter.printHelp(" ", getStopOptionsWithoutDeprecatedOptions(new Options()));

		printCustomCliOptions(formatter, false);

		System.out.println();
	}

	public static void printHelpForCancel() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setLeftPadding(5);
		formatter.setWidth(80);

		System.out.println("\nAction \"cancel\" cancels a running program.");
		System.out.println("\n  Syntax: cancel [OPTIONS] <Job ID>");
		formatter.setSyntaxPrefix("  \"cancel\" action options:");
		formatter.printHelp(" ", getCancelOptionsWithoutDeprecatedOptions(new Options()));

		printCustomCliOptions(formatter, false);

		System.out.println();
	}

	public static void printHelpForSavepoint() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setLeftPadding(5);
		formatter.setWidth(80);

		System.out.println("\nAction \"savepoint\" triggers savepoints for a running job or disposes existing ones.");
		System.out.println("\n  Syntax: savepoint [OPTIONS] <Job ID>");
		formatter.setSyntaxPrefix("  \"savepoint\" action options:");
		formatter.printHelp(" ", getSavepointOptionsWithoutDeprecatedOptions(new Options()));

		printCustomCliOptions(formatter, false);

		System.out.println();
	}

	/**
	 * Adds custom cli options
	 * @param options The options to add options to
	 * @param runOptions Whether to include run options
	 * @return Options with additions
	 */
	private static Options addCustomCliOptions(Options options, boolean runOptions) {
		for (CustomCommandLine cli: CliFrontend.getCustomCommandLineList()) {
			cli.addGeneralOptions(options);
			if (runOptions) {
				cli.addRunOptions(options);
			}
		}
		return options;
	}

	/**
	 * Prints custom cli options
	 * @param formatter The formatter to use for printing
	 * @param runOptions True if the run options should be printed, False to print only general options
	 */
	private static void printCustomCliOptions(HelpFormatter formatter, boolean runOptions) {
		// prints options from all available command-line classes
		for (CustomCommandLine cli: CliFrontend.getCustomCommandLineList()) {
			if (cli.getId() != null) {
				formatter.setSyntaxPrefix("  Options for " + cli.getId() + " mode:");
				Options customOpts = new Options();
				cli.addGeneralOptions(customOpts);
				if (runOptions) {
					cli.addRunOptions(customOpts);
				}
				formatter.printHelp(" ", customOpts);
				System.out.println();
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Line Parsing
	// --------------------------------------------------------------------------------------------

	public static RunOptions parseRunCommand(String[] args) throws CliArgsException {
		try {
			DefaultParser parser = new DefaultParser();
			CommandLine line = parser.parse(RUN_OPTIONS, args, true);
			return new RunOptions(line);
		}
		catch (ParseException e) {
			throw new CliArgsException(e.getMessage());
		}
	}

	public static ListOptions parseListCommand(String[] args) throws CliArgsException {
		try {
			DefaultParser parser = new DefaultParser();
			CommandLine line = parser.parse(LIST_OPTIONS, args, false);
			return new ListOptions(line);
		}
		catch (ParseException e) {
			throw new CliArgsException(e.getMessage());
		}
	}

	public static CancelOptions parseCancelCommand(String[] args) throws CliArgsException {
		try {
			DefaultParser parser = new DefaultParser();
			CommandLine line = parser.parse(CANCEL_OPTIONS, args, false);
			return new CancelOptions(line);
		}
		catch (ParseException e) {
			throw new CliArgsException(e.getMessage());
		}
	}

	public static StopOptions parseStopCommand(String[] args) throws CliArgsException {
		try {
			DefaultParser parser = new DefaultParser();
			CommandLine line = parser.parse(STOP_OPTIONS, args, false);
			return new StopOptions(line);
		} catch (ParseException e) {
			throw new CliArgsException(e.getMessage());
		}
	}

	public static SavepointOptions parseSavepointCommand(String[] args) throws CliArgsException {
		try {
			DefaultParser parser = new DefaultParser();
			CommandLine line = parser.parse(SAVEPOINT_OPTIONS, args, false);
			return new SavepointOptions(line);
		}
		catch (ParseException e) {
			throw new CliArgsException(e.getMessage());
		}
	}

	public static InfoOptions parseInfoCommand(String[] args) throws CliArgsException {
		try {
			DefaultParser parser = new DefaultParser();
			CommandLine line = parser.parse(INFO_OPTIONS, args, true);
			return new InfoOptions(line);
		}
		catch (ParseException e) {
			throw new CliArgsException(e.getMessage());
		}
	}

}
