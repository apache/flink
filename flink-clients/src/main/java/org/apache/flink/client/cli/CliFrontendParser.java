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
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import org.apache.flink.client.CliFrontend;
import org.apache.flink.client.FlinkYarnSessionCli;

/**
 * A simple command line parser (based on Apache Commons CLI) that extracts command
 * line options.
 */
public class CliFrontendParser {

	/** command line interface of the YARN session, with a special initialization here
	 *  to prefix all options with y/yarn. */
	private static final FlinkYarnSessionCli yarnSessionCLi = new FlinkYarnSessionCli("y", "yarn");


	static final Option HELP_OPTION = new Option("h", "help", false,
												"Show the help message for the CLI Frontend or the action.");

	static final Option JAR_OPTION = new Option("j", "jarfile", true, "Flink program JAR file.");

	public static final Option CLASS_OPTION = new Option("c", "class", true,
			"Class with the program entry point (\"main\" method or \"getPlan()\" method. Only needed if the " +
					"JAR file does not specify the class in its manifest.");

	static final Option CLASSPATH_OPTION = new Option("C", "classpath", true, "Adds a URL to each user code " +
			"classloader  on all nodes in the cluster. The paths must specify a protocol (e.g. file://) and be " +
					"accessible on all nodes (e.g. by means of a NFS share). You can use this option multiple " +
					"times for specifying more than one URL. The protocol must be supported by the " +
					"{@link java.net.URLClassLoader}.");

	static final Option PARALLELISM_OPTION = new Option("p", "parallelism", true,
			"The parallelism with which to run the program. Optional flag to override the default value " +
					"specified in the configuration.");

	static final Option LOGGING_OPTION = new Option("q", "sysoutLogging", false, "If present, " +
			"supress logging output to standard out.");

	static final Option DETACHED_OPTION = new Option("d", "detached", false, "If present, runs " +
			"the job in detached mode");

	static final Option ARGS_OPTION = new Option("a", "arguments", true,
			"Program arguments. Arguments can also be added without -a, simply as trailing parameters.");

	static final Option ADDRESS_OPTION = new Option("m", "jobmanager", true,
			"Address of the JobManager (master) to which to connect. Specify '" + CliFrontend.YARN_DEPLOY_JOBMANAGER
					+ "' as the JobManager to deploy a YARN cluster for the job. Use this flag to connect to a " +
					"different JobManager than the one specified in the configuration.");

	static final Option SAVEPOINT_PATH_OPTION = new Option("s", "fromSavepoint", true,
			"Path to a savepoint to reset the job back to (for example file:///flink/savepoint-1537).");

	static final Option SAVEPOINT_DISPOSE_OPTION = new Option("d", "dispose", true,
			"Disposes an existing savepoint.");

	// list specific options
	static final Option RUNNING_OPTION = new Option("r", "running", false,
			"Show only running programs and their JobIDs");

	static final Option SCHEDULED_OPTION = new Option("s", "scheduled", false,
			"Show only scheduled programs and their JobIDs");

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

		SAVEPOINT_DISPOSE_OPTION.setRequired(false);
		SAVEPOINT_DISPOSE_OPTION.setArgName("savepointPath");
	}

	private static final Options RUN_OPTIONS = getRunOptions(buildGeneralOptions(new Options()));
	private static final Options INFO_OPTIONS = getInfoOptions(buildGeneralOptions(new Options()));
	private static final Options LIST_OPTIONS = getListOptions(buildGeneralOptions(new Options()));
	private static final Options CANCEL_OPTIONS = getCancelOptions(buildGeneralOptions(new Options()));
	private static final Options SAVEPOINT_OPTIONS = getSavepointOptions(buildGeneralOptions(new Options()));

	private static Options buildGeneralOptions(Options options) {
		options.addOption(HELP_OPTION);
		// backwards compatibility: ignore verbose flag (-v)
		options.addOption(new Option("v", "verbose", false, "This option is deprecated."));
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

		// also add the YARN options so that the parser can parse them
		yarnSessionCLi.getYARNSessionCLIOptions(options);
		return options;
	}

	private static Options getProgramSpecificOptionsWithoutDeprecatedOptions(Options options) {
		options.addOption(CLASS_OPTION);
		options.addOption(CLASSPATH_OPTION);
		options.addOption(PARALLELISM_OPTION);
		options.addOption(LOGGING_OPTION);
		options.addOption(DETACHED_OPTION);
		options.addOption(SAVEPOINT_PATH_OPTION);
		return options;
	}

	private static Options getRunOptions(Options options) {
		Options o = getProgramSpecificOptions(options);
		return getJobManagerAddressOption(o);
	}

	private static Options getRunOptionsWithoutDeprecatedOptions(Options options) {
		Options o = getProgramSpecificOptionsWithoutDeprecatedOptions(options);
		return getJobManagerAddressOption(o);
	}

	private static Options getJobManagerAddressOption(Options options) {
		options.addOption(ADDRESS_OPTION);
		return options;
	}

	private static Options getInfoOptions(Options options) {
		options = getProgramSpecificOptions(options);
		options = getJobManagerAddressOption(options);
		return options;
	}

	private static Options getInfoOptionsWithoutDeprecatedOptions(Options options) {
		options.addOption(CLASS_OPTION);
		options.addOption(PARALLELISM_OPTION);
		options = getJobManagerAddressOption(options);
		return options;
	}

	private static Options getListOptions(Options options) {
		options.addOption(RUNNING_OPTION);
		options.addOption(SCHEDULED_OPTION);
		options = getJobManagerAddressOption(options);
		return options;
	}

	private static Options getCancelOptions(Options options) {
		options = getJobManagerAddressOption(options);
		return options;
	}

	private static Options getSavepointOptions(Options options) {
		options = getJobManagerAddressOption(options);
		options.addOption(SAVEPOINT_DISPOSE_OPTION);
		return options;
	}

	// --------------------------------------------------------------------------------------------
	//  Help
	// --------------------------------------------------------------------------------------------

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
		formatter.setSyntaxPrefix("  Additional arguments if -m " + CliFrontend.YARN_DEPLOY_JOBMANAGER + " is set:");
		Options yarnOpts = new Options();
		yarnSessionCLi.getYARNSessionCLIOptions(yarnOpts);
		formatter.printHelp(" ", yarnOpts);
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
		System.out.println();
	}

	public static void printHelpForList() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setLeftPadding(5);
		formatter.setWidth(80);

		System.out.println("\nAction \"list\" lists running and scheduled programs.");
		System.out.println("\n  Syntax: list [OPTIONS]");
		formatter.setSyntaxPrefix("  \"list\" action options:");
		formatter.printHelp(" ", getListOptions(new Options()));
		System.out.println();
	}

	public static void printHelpForCancel() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setLeftPadding(5);
		formatter.setWidth(80);

		System.out.println("\nAction \"cancel\" cancels a running program.");
		System.out.println("\n  Syntax: cancel [OPTIONS] <Job ID>");
		formatter.setSyntaxPrefix("  \"cancel\" action options:");
		formatter.printHelp(" ", getCancelOptions(new Options()));
		System.out.println();
	}

	public static void printHelpForSavepoint() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setLeftPadding(5);
		formatter.setWidth(80);

		System.out.println("\nAction \"savepoint\" triggers savepoints for a running job or disposes existing ones.");
		System.out.println("\n  Syntax: savepoint [OPTIONS] <Job ID>");
		formatter.setSyntaxPrefix("  \"savepoint\" action options:");
		formatter.printHelp(" ", getSavepointOptions(new Options()));
		System.out.println();
	}

	// --------------------------------------------------------------------------------------------
	//  Line Parsing
	// --------------------------------------------------------------------------------------------

	public static RunOptions parseRunCommand(String[] args) throws CliArgsException {
		try {
			PosixParser parser = new PosixParser();
			CommandLine line = parser.parse(RUN_OPTIONS, args, true);
			return new RunOptions(line);
		}
		catch (ParseException e) {
			throw new CliArgsException(e.getMessage());
		}
	}

	public static ListOptions parseListCommand(String[] args) throws CliArgsException {
		try {
			PosixParser parser = new PosixParser();
			CommandLine line = parser.parse(LIST_OPTIONS, args, false);
			return new ListOptions(line);
		}
		catch (ParseException e) {
			throw new CliArgsException(e.getMessage());
		}
	}

	public static CancelOptions parseCancelCommand(String[] args) throws CliArgsException {
		try {
			PosixParser parser = new PosixParser();
			CommandLine line = parser.parse(CANCEL_OPTIONS, args, false);
			return new CancelOptions(line);
		}
		catch (ParseException e) {
			throw new CliArgsException(e.getMessage());
		}
	}

	public static SavepointOptions parseSavepointCommand(String[] args) throws CliArgsException {
		try {
			PosixParser parser = new PosixParser();
			CommandLine line = parser.parse(SAVEPOINT_OPTIONS, args, false);
			return new SavepointOptions(line);
		}
		catch (ParseException e) {
			throw new CliArgsException(e.getMessage());
		}
	}

	public static InfoOptions parseInfoCommand(String[] args) throws CliArgsException {
		try {
			PosixParser parser = new PosixParser();
			CommandLine line = parser.parse(INFO_OPTIONS, args, false);
			return new InfoOptions(line);
		}
		catch (ParseException e) {
			throw new CliArgsException(e.getMessage());
		}
	}

	public static FlinkYarnSessionCli getFlinkYarnSessionCli() {
		return yarnSessionCLi;
	}
}
