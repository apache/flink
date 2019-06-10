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

package org.apache.flink.client.python;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.util.ArrayList;
import java.util.List;

/**
 * Command line parser for Python shell.
 */
public class PythonShellParser {
	private static final Option OPTION_HELP = Option
		.builder("h")
		.required(false)
		.longOpt("help")
		.desc("Show the help message with descriptions of all options.")
		.build();

	private static final Option OPTION_CONTAINER = Option
		.builder("n")
		.required(false)
		.longOpt("container")
		.hasArg()
		.desc("Number of YARN container to allocate (=Number of Task Managers)")
		.build();

	private static final Option OPTION_JM_MEMORY = Option
		.builder("jm")
		.required(false)
		.longOpt("jobManagerMemory")
		.hasArg()
		.desc("Memory for JobManager Container with optional unit (default: MB)")
		.build();

	private static final Option OPTION_NAME = Option
		.builder("nm")
		.required(false)
		.longOpt("name")
		.hasArg()
		.desc("Set a custom name for the application on YARN")
		.build();

	private static final Option OPTION_QUEUE = Option
		.builder("qu")
		.required(false)
		.longOpt("queue")
		.hasArg()
		.desc("Specify YARN queue.")
		.build();

	private static final Option OPTION_SLOTS = Option
		.builder("s")
		.required(false)
		.longOpt("slots")
		.hasArg()
		.desc("Number of slots per TaskManager")
		.build();

	private static final Option OPTION_TM_MEMORY = Option
		.builder("tm")
		.required(false)
		.longOpt("taskManagerMemory")
		.hasArg()
		.desc("Memory per TaskManager Container with optional unit (default: MB)")
		.build();

	// cluster types
	private static final String LOCAL_RUN = "local";
	private static final String REMOTE_RUN = "remote";
	private static final String YARN_RUN = "yarn";

	// Options that will be used in mini cluster.
	private static final Options LOCAL_OPTIONS = getLocalOptions(new Options());

	// Options that will be used in remote cluster.
	private static final Options REMOTE_OPTIONS = getRemoteOptions(new Options());

	// Options that will be used in yarn cluster.
	private static final Options YARN_OPTIONS = getYarnOptions(new Options());

	public static void main(String[] args) {
		if (args.length < 1) {
			printError("You should specify cluster type or -h | --help option");
			System.exit(1);
		}
		String command = args[0];
		List<String> commandOptions = null;
		try {
			switch (command) {
				case LOCAL_RUN:
					commandOptions = parseLocal(args);
					break;
				case REMOTE_RUN:
					commandOptions = parseRemote(args);
					break;
				case YARN_RUN:
					commandOptions = parseYarn(args);
					break;
				case "-h":
				case "--help":
					printHelp();
					break;
				default:
					printError(String.format("\"%s\" is not a valid cluster type or -h | --help option.\n", command));
					System.exit(1);
			}
			if (commandOptions != null) {
				for (String option : commandOptions) {
					System.out.print(option);
					System.out.print('\0');
				}
			}
		} catch (Throwable e) {
			printError("Error while running the command.");
			e.printStackTrace();
			System.exit(1);
		}
	}

	private static void buildGeneralOptions(Options options) {
		options.addOption(OPTION_HELP);
	}

	private static Options getLocalOptions(Options options) {
		buildGeneralOptions(options);
		return options;
	}

	private static Options getRemoteOptions(Options options) {
		buildGeneralOptions(options);
		return options;
	}

	private static Options getYarnOptions(Options options) {
		buildGeneralOptions(options);
		options.addOption(OPTION_CONTAINER);
		options.addOption(OPTION_JM_MEMORY);
		options.addOption(OPTION_NAME);
		options.addOption(OPTION_QUEUE);
		options.addOption(OPTION_SLOTS);
		options.addOption(OPTION_TM_MEMORY);
		return options;
	}

	/**
	 * Prints the error message and help for the client.
	 *
	 * @param msg error message
	 */
	private static void printError(String msg) {
		System.err.println(msg);
		System.err.println("Valid cluster type are \"local\", \"remote <hostname> <portnumber>\", \"yarn\".");
		System.err.println();
		System.err.println("Specify the help option (-h or --help) to get help on the command.");
	}

	/**
	 * Prints the help for the client.
	 */
	private static void printHelp() {
		System.out.print("Flink Python Shell\n");
		System.out.print("Usage: pyflink-shell.sh [local|remote|yarn] [options] <args>...\n");
		System.out.print('\n');
		printLocalHelp();
		printRemoteHelp();
		printYarnHelp();
		System.out.println("-h | --help");
		System.out.println("      Prints this usage text");
		System.exit(0);
	}

	private static void printYarnHelp() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setLeftPadding(5);
		formatter.setWidth(80);
		System.out.println("Command: yarn [options]");
		System.out.println("Starts Flink Python shell connecting to a yarn cluster");
		formatter.printHelp(" ", YARN_OPTIONS);
	}

	private static void printRemoteHelp() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setLeftPadding(5);
		formatter.setWidth(80);
		System.out.println("Command: remote [options] <host> <port>");
		System.out.println("Starts Flink Python shell connecting to a remote cluster");
		System.out.println("  <host>");
		System.out.println("        Remote host name as string");
		System.out.println("  <port>");
		System.out.println("        Remote port as integer");
		System.out.println();
		formatter.printHelp(" ", REMOTE_OPTIONS);
	}

	private static void printLocalHelp() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setLeftPadding(5);
		formatter.setWidth(80);
		System.out.println("Command: local [options]");
		System.out.println("Starts Flink Python shell with a local Flink cluster");
		formatter.printHelp(" ", LOCAL_OPTIONS);
	}

	/**
	 * Constructs yarn options. The python shell option will add prefix 'y' to align yarn options in `flink run`.
	 *
	 * @param options     Options that will be used in `flink run`.
	 * @param yarnOption  Python shell yarn options.
	 * @param commandLine Parsed Python shell parser options.
	 */
	private static void constructYarnOption(List<String> options, Option yarnOption, CommandLine commandLine) {
		if (commandLine.hasOption(yarnOption.getOpt())) {
			options.add("-y" + yarnOption.getOpt());
			options.add(commandLine.getOptionValue(yarnOption.getOpt()));
		}
	}

	/**
	 * Parses Python shell yarn options and transfer to yarn options which will be used in `flink run` to
	 * submit flink job.
	 *
	 * @param args Python shell yarn options.
	 * @return Yarn options usrd in `flink run`.
	 */
	static List<String> parseYarn(String[] args) {
		String[] params = new String[args.length - 1];
		System.arraycopy(args, 1, params, 0, params.length);
		CommandLine commandLine = parse(YARN_OPTIONS, params);
		if (commandLine.hasOption(OPTION_HELP.getOpt())) {
			printYarnHelp();
			System.exit(0);
		}
		List<String> options = new ArrayList<>();
		options.add(args[0]);
		options.add("-m");
		options.add("yarn-cluster");
		constructYarnOption(options, OPTION_CONTAINER, commandLine);
		constructYarnOption(options, OPTION_JM_MEMORY, commandLine);
		constructYarnOption(options, OPTION_NAME, commandLine);
		constructYarnOption(options, OPTION_QUEUE, commandLine);
		constructYarnOption(options, OPTION_SLOTS, commandLine);
		constructYarnOption(options, OPTION_TM_MEMORY, commandLine);
		return options;
	}

	/**
	 * Parses Python shell options and transfer to options which will be used in `flink run -m ${jobmanager_address}` to
	 * submit flink job in a remote jobmanager.
	 * The Python shell options "remote ${hostname} ${portnumber}" will be transferred to "-m ${hostname}:${portnumber}".
	 *
	 * @param args Python shell options.
	 * @return Options used in `flink run`.
	 */
	static List<String> parseRemote(String[] args) {
		if (args.length < 3) {
			System.err.println("Specifies the <hostname> <portnumber> in 'remote' mode");
			printRemoteHelp();
			System.exit(0);
		}
		String[] params = new String[args.length - 3];
		System.arraycopy(args, 3, params, 0, params.length);
		CommandLine commandLine = parse(REMOTE_OPTIONS, params);
		if (commandLine.hasOption(OPTION_HELP.getOpt())) {
			printRemoteHelp();
			System.exit(0);
		}
		String host = args[1];
		String port = args[2];
		List<String> options = new ArrayList<>();
		options.add(args[0]);
		options.add("-m");
		options.add(host + ":" + port);
		return options;
	}

	/**
	 * Parses Python shell options and transfer to options which will be used in `java` to exec a flink job in
	 * local mini cluster.
	 *
	 * @param args Python shell options.
	 * @return Options used in `java` run.
	 */
	static List<String> parseLocal(String[] args) {
		String[] params = new String[args.length - 1];
		System.arraycopy(args, 1, params, 0, params.length);
		CommandLine commandLine = parse(LOCAL_OPTIONS, params);
		if (commandLine.hasOption(OPTION_HELP.getOpt())) {
			printLocalHelp();
			System.exit(0);
		}
		List<String> options = new ArrayList<>();
		options.add("local");
		return options;
	}

	private static CommandLine parse(Options options, String[] args) {
		final DefaultParser parser = new DefaultParser();
		try {
			return parser.parse(options, args, true);
		} catch (ParseException e) {
			throw new RuntimeException("Parser parses options failed.", e);
		}
	}
}
