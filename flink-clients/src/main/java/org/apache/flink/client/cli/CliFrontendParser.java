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

import org.apache.flink.configuration.CheckpointingOptions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import javax.annotation.Nullable;

import java.util.Collection;

/**
 * A simple command line parser (based on Apache Commons CLI) that extracts command
 * line options.
 */
public class CliFrontendParser {

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

	/**
	 * @deprecated use non-prefixed variant {@link #DETACHED_OPTION} for both YARN and non-YARN deployments
	 */
	@Deprecated
	public static final Option YARN_DETACHED_OPTION = new Option("yd", "yarndetached", false, "If present, runs " +
		"the job in detached mode (deprecated; use non-YARN specific option instead)");

	static final Option ARGS_OPTION = new Option("a", "arguments", true,
			"Program arguments. Arguments can also be added without -a, simply as trailing parameters.");

	public static final Option ADDRESS_OPTION = new Option("m", "jobmanager", true,
			"Address of the JobManager (master) to which to connect. " +
			"Use this flag to connect to a different JobManager than the one specified in the configuration.");

	static final Option SAVEPOINT_PATH_OPTION = new Option("s", "fromSavepoint", true,
			"Path to a savepoint to restore the job from (for example hdfs:///flink/savepoint-1537).");

	static final Option SAVEPOINT_ALLOW_NON_RESTORED_OPTION = new Option("n", "allowNonRestoredState", false,
			"Allow to skip savepoint state that cannot be restored. " +
					"You need to allow this if you removed an operator from your " +
					"program that was part of the program when the savepoint was triggered.");

	static final Option SAVEPOINT_DISPOSE_OPTION = new Option("d", "dispose", true,
			"Path of savepoint to dispose.");

	// list specific options
	static final Option RUNNING_OPTION = new Option("r", "running", false,
			"Show only running programs and their JobIDs");

	static final Option SCHEDULED_OPTION = new Option("s", "scheduled", false,
			"Show only scheduled programs and their JobIDs");

	static final Option ALL_OPTION = new Option("a", "all", false,
		"Show all programs and their JobIDs");

	static final Option ZOOKEEPER_NAMESPACE_OPTION = new Option("z", "zookeeperNamespace", true,
			"Namespace to create the Zookeeper sub-paths for high availability mode");

	static final Option CANCEL_WITH_SAVEPOINT_OPTION = new Option(
			"s", "withSavepoint", true, "Trigger savepoint and cancel job. The target " +
			"directory is optional. If no directory is specified, the configured default " +
			"directory (" + CheckpointingOptions.SAVEPOINT_DIRECTORY.key() + ") is used.");

	static final Option MODIFY_PARALLELISM_OPTION = new Option("p", "parallelism", true, "New parallelism for the specified job.");

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
		YARN_DETACHED_OPTION.setRequired(false);

		ARGS_OPTION.setRequired(false);
		ARGS_OPTION.setArgName("programArgs");
		ARGS_OPTION.setArgs(Option.UNLIMITED_VALUES);

		RUNNING_OPTION.setRequired(false);
		SCHEDULED_OPTION.setRequired(false);

		SAVEPOINT_PATH_OPTION.setRequired(false);
		SAVEPOINT_PATH_OPTION.setArgName("savepointPath");

		SAVEPOINT_ALLOW_NON_RESTORED_OPTION.setRequired(false);

		ZOOKEEPER_NAMESPACE_OPTION.setRequired(false);
		ZOOKEEPER_NAMESPACE_OPTION.setArgName("zookeeperNamespace");

		CANCEL_WITH_SAVEPOINT_OPTION.setRequired(false);
		CANCEL_WITH_SAVEPOINT_OPTION.setArgName("targetDirectory");
		CANCEL_WITH_SAVEPOINT_OPTION.setOptionalArg(true);

		MODIFY_PARALLELISM_OPTION.setRequired(false);
		MODIFY_PARALLELISM_OPTION.setArgName("newParallelism");
	}

	private static final Options RUN_OPTIONS = getRunCommandOptions();

	private static Options buildGeneralOptions(Options options) {
		options.addOption(HELP_OPTION);
		// backwards compatibility: ignore verbose flag (-v)
		options.addOption(new Option("v", "verbose", false, "This option is deprecated."));
		return options;
	}

	private static Options getProgramSpecificOptions(Options options) {
		options.addOption(JAR_OPTION);
		options.addOption(CLASS_OPTION);
		options.addOption(CLASSPATH_OPTION);
		options.addOption(PARALLELISM_OPTION);
		options.addOption(ARGS_OPTION);
		options.addOption(LOGGING_OPTION);
		options.addOption(DETACHED_OPTION);
		options.addOption(YARN_DETACHED_OPTION);
		return options;
	}

	private static Options getProgramSpecificOptionsWithoutDeprecatedOptions(Options options) {
		options.addOption(CLASS_OPTION);
		options.addOption(CLASSPATH_OPTION);
		options.addOption(PARALLELISM_OPTION);
		options.addOption(LOGGING_OPTION);
		options.addOption(DETACHED_OPTION);
		return options;
	}

	public static Options getRunCommandOptions() {
		Options options = buildGeneralOptions(new Options());
		options = getProgramSpecificOptions(options);
		options.addOption(SAVEPOINT_PATH_OPTION);
		return options.addOption(SAVEPOINT_ALLOW_NON_RESTORED_OPTION);
	}

	static Options getInfoCommandOptions() {
		Options options = buildGeneralOptions(new Options());
		return getProgramSpecificOptions(options);
	}

	static Options getListCommandOptions() {
		Options options = buildGeneralOptions(new Options());
		options.addOption(ALL_OPTION);
		options.addOption(RUNNING_OPTION);
		return options.addOption(SCHEDULED_OPTION);
	}

	static Options getCancelCommandOptions() {
		Options options = buildGeneralOptions(new Options());
		return options.addOption(CANCEL_WITH_SAVEPOINT_OPTION);
	}

	static Options getStopCommandOptions() {
		return buildGeneralOptions(new Options());
	}

	static Options getSavepointCommandOptions() {
		Options options = buildGeneralOptions(new Options());
		options.addOption(SAVEPOINT_DISPOSE_OPTION);
		return options.addOption(JAR_OPTION);
	}

	static Options getModifyOptions() {
		final Options options = buildGeneralOptions(new Options());
		options.addOption(MODIFY_PARALLELISM_OPTION);
		return options;
	}

	// --------------------------------------------------------------------------------------------
	//  Help
	// --------------------------------------------------------------------------------------------

	private static Options getRunOptionsWithoutDeprecatedOptions(Options options) {
		Options o = getProgramSpecificOptionsWithoutDeprecatedOptions(options);
		o.addOption(SAVEPOINT_PATH_OPTION);
		return o.addOption(SAVEPOINT_ALLOW_NON_RESTORED_OPTION);
	}

	private static Options getInfoOptionsWithoutDeprecatedOptions(Options options) {
		options.addOption(CLASS_OPTION);
		options.addOption(PARALLELISM_OPTION);
		return options;
	}

	private static Options getListOptionsWithoutDeprecatedOptions(Options options) {
		options.addOption(RUNNING_OPTION);
		return options.addOption(SCHEDULED_OPTION);
	}

	private static Options getCancelOptionsWithoutDeprecatedOptions(Options options) {
		return options.addOption(CANCEL_WITH_SAVEPOINT_OPTION);
	}

	private static Options getStopOptionsWithoutDeprecatedOptions(Options options) {
		return options;
	}

	private static Options getSavepointOptionsWithoutDeprecatedOptions(Options options) {
		options.addOption(SAVEPOINT_DISPOSE_OPTION);
		options.addOption(JAR_OPTION);
		return options;
	}

	/**
	 * Prints the help for the client.
	 */
	public static void printHelp(Collection<CustomCommandLine<?>> customCommandLines) {
		System.out.println("./flink <ACTION> [OPTIONS] [ARGUMENTS]");
		System.out.println();
		System.out.println("The following actions are available:");

		printHelpForRun(customCommandLines);
		printHelpForInfo();
		printHelpForList(customCommandLines);
		printHelpForStop(customCommandLines);
		printHelpForCancel(customCommandLines);
		printHelpForSavepoint(customCommandLines);
		printHelpForModify(customCommandLines);

		System.out.println();
	}

	public static void printHelpForRun(Collection<CustomCommandLine<?>> customCommandLines) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setLeftPadding(5);
		formatter.setWidth(80);

		System.out.println("\nAction \"run\" compiles and runs a program.");
		System.out.println("\n  Syntax: run [OPTIONS] <jar-file> <arguments>");
		formatter.setSyntaxPrefix("  \"run\" action options:");
		formatter.printHelp(" ", getRunOptionsWithoutDeprecatedOptions(new Options()));

		printCustomCliOptions(customCommandLines, formatter, true);

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

	public static void printHelpForList(Collection<CustomCommandLine<?>> customCommandLines) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setLeftPadding(5);
		formatter.setWidth(80);

		System.out.println("\nAction \"list\" lists running and scheduled programs.");
		System.out.println("\n  Syntax: list [OPTIONS]");
		formatter.setSyntaxPrefix("  \"list\" action options:");
		formatter.printHelp(" ", getListOptionsWithoutDeprecatedOptions(new Options()));

		printCustomCliOptions(customCommandLines, formatter, false);

		System.out.println();
	}

	public static void printHelpForStop(Collection<CustomCommandLine<?>> customCommandLines) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setLeftPadding(5);
		formatter.setWidth(80);

		System.out.println("\nAction \"stop\" stops a running program (streaming jobs only).");
		System.out.println("\n  Syntax: stop [OPTIONS] <Job ID>");
		formatter.setSyntaxPrefix("  \"stop\" action options:");
		formatter.printHelp(" ", getStopOptionsWithoutDeprecatedOptions(new Options()));

		printCustomCliOptions(customCommandLines, formatter, false);

		System.out.println();
	}

	public static void printHelpForCancel(Collection<CustomCommandLine<?>> customCommandLines) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setLeftPadding(5);
		formatter.setWidth(80);

		System.out.println("\nAction \"cancel\" cancels a running program.");
		System.out.println("\n  Syntax: cancel [OPTIONS] <Job ID>");
		formatter.setSyntaxPrefix("  \"cancel\" action options:");
		formatter.printHelp(" ", getCancelOptionsWithoutDeprecatedOptions(new Options()));

		printCustomCliOptions(customCommandLines, formatter, false);

		System.out.println();
	}

	public static void printHelpForSavepoint(Collection<CustomCommandLine<?>> customCommandLines) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setLeftPadding(5);
		formatter.setWidth(80);

		System.out.println("\nAction \"savepoint\" triggers savepoints for a running job or disposes existing ones.");
		System.out.println("\n  Syntax: savepoint [OPTIONS] <Job ID> [<target directory>]");
		formatter.setSyntaxPrefix("  \"savepoint\" action options:");
		formatter.printHelp(" ", getSavepointOptionsWithoutDeprecatedOptions(new Options()));

		printCustomCliOptions(customCommandLines, formatter, false);

		System.out.println();
	}

	public static void printHelpForModify(Collection<CustomCommandLine<?>> customCommandLines) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setLeftPadding(5);
		formatter.setWidth(80);

		System.out.println("\nAction \"modify\" modifies a running job (e.g. change of parallelism).");
		System.out.println("\n  Syntax: modify <Job ID> [OPTIONS]");
		formatter.setSyntaxPrefix("  \"modify\" action options:");
		formatter.printHelp(" ", getModifyOptions());

		printCustomCliOptions(customCommandLines, formatter, false);

		System.out.println();
	}

	/**
	 * Prints custom cli options.
	 * @param formatter The formatter to use for printing
	 * @param runOptions True if the run options should be printed, False to print only general options
	 */
	private static void printCustomCliOptions(
			Collection<CustomCommandLine<?>> customCommandLines,
			HelpFormatter formatter,
			boolean runOptions) {
		// prints options from all available command-line classes
		for (CustomCommandLine cli: customCommandLines) {
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

	public static CommandLine parse(Options options, String[] args, boolean stopAtNonOptions) throws CliArgsException {
		final DefaultParser parser = new DefaultParser();

		try {
			return parser.parse(options, args, stopAtNonOptions);
		} catch (ParseException e) {
			throw new CliArgsException(e.getMessage());
		}
	}

	/**
	 * Merges the given {@link Options} into a new Options object.
	 *
	 * @param optionsA options to merge, can be null if none
	 * @param optionsB options to merge, can be null if none
	 * @return
	 */
	public static Options mergeOptions(@Nullable Options optionsA, @Nullable Options optionsB) {
		final Options resultOptions = new Options();
		if (optionsA != null) {
			for (Option option : optionsA.getOptions()) {
				resultOptions.addOption(option);
			}
		}

		if (optionsB != null) {
			for (Option option : optionsB.getOptions()) {
				resultOptions.addOption(option);
			}
		}

		return resultOptions;
	}
}
