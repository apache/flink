/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.client;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.pact.client.nephele.api.Client;
import eu.stratosphere.pact.client.nephele.api.ErrorInPlanAssemblerException;
import eu.stratosphere.pact.client.nephele.api.PactProgram;
import eu.stratosphere.pact.client.nephele.api.ProgramInvocationException;

/**
 * Implementation of a simple command line fronted for executing PACT programs.
 * 
 * @author Moritz Kaufmann
 */
public class CliFrontend {

	private static final Option CLASS_OPTION = new Option("c", "class", true, "Pact plan assembler class");

	private static final Option DESCR_OPTION = new Option("d", "description", false, "Show description of pact plan. "
		+ "Plan will not be executed");

	private static final Option HELP_OPTION = new Option("h", "help", false, "Show the help for the CLI Frontend.");

	private static final Option VERBOSE_OPTION = new Option("v", "verbose", false,
		"Print more detailed error messages.");

	private static final String DEFAULT_CONFIG_DIRECTORY = "../conf";

	private static final String ENV_CONFIG_DIRECTORY = "NEPHELE_CONF_DIR";

	private CommandLineParser parser;

	private Options options;

	private boolean verbose;

	/**
	 * Initializes the class
	 */
	public CliFrontend() {
		parser = new PosixParser();
		options = initOptions();
	}

	/**
	 * Executes the pact program defined in the parameters.
	 * 
	 * @param args
	 * @throws ParseException
	 */
	public void run(String[] args) throws ParseException {
		// Parse command line options
		CommandLine line = null;
		try {
			line = parser.parse(options, args, true);
		} catch (Exception e) {
			handleError(e);
		}

		verbose = line.hasOption(VERBOSE_OPTION.getOpt());

		// Show CLI help if desired
		if (line.hasOption(HELP_OPTION.getOpt())) {
			printHelp();
			System.exit(0);
		}

		// Get pact program arguments
		String[] program = line.getArgs();
		if (program.length < 1) {
			System.out.println("Error: Jar file is not set");
			printHelp();
			System.exit(1);
		}
		String[] pactArgs = new String[program.length - 1];
		System.arraycopy(program, 1, pactArgs, 0, pactArgs.length);

		// Get jar file argument
		String jarPath = program[0];
		File jar = new File(jarPath);

		// Get class argument
		String clazz = line.getOptionValue(CLASS_OPTION.getOpt());

		// Try to get load plan
		PactProgram prog = null;
		try {
			if (clazz == null) {
				prog = new PactProgram(jar, pactArgs);
			} else {
				prog = new PactProgram(jar, clazz, pactArgs);
			}
		} catch (ProgramInvocationException e) {
			handleError(e);
		}

		// TODO: Check if jar contains the plan
		if (line.hasOption(DESCR_OPTION.getOpt())) {
			String descr = null;
			try {
				descr = prog.getTextDescription();
			} catch (Exception e) {
				handleError(e);
			}

			if (descr != null) {
				System.out.println(descr);
			} else {
				System.out.println("No description available for this plan.");
			}
			System.exit(0);
		}

		Configuration configuration = getConfiguration();
		// TODO: What happens if configuration cannot be read?
		Client client = new Client(configuration);
		try {
			client.run(prog);
		} catch (ProgramInvocationException e) {
			handleError(e);
		} catch (ErrorInPlanAssemblerException e) {
			handleError(e);
		}

		System.out.println("Job successfully submitted");
	}

	private void handleError(Exception e) {
		System.out.println(e.getMessage());
		if (verbose) {
			e.printStackTrace();
		} else {
			System.out.println("For a more detailed error message use the '-v' option");
		}
		System.exit(1);
	}

	protected Options initOptions() {
		CLASS_OPTION.setRequired(false);
		CLASS_OPTION.setArgName("classname");
		DESCR_OPTION.setRequired(false);
		HELP_OPTION.setRequired(false);
		VERBOSE_OPTION.setRequired(false);

		// create the Options
		Options options = new Options();
		options.addOption(CLASS_OPTION);
		options.addOption(DESCR_OPTION);
		options.addOption(HELP_OPTION);
		options.addOption(VERBOSE_OPTION);

		return options;
	}

	protected void printHelp() {
		HelpFormatter formatter = new HelpFormatter();
		// formatter.printHelp("pact-run [OPTIONS] JARFILE [PACT ARGS]", "blabla", options, "blubb");
		formatter.printHelp("pact-run [OPTIONS] JARFILE [PACT ARGS]", options);
	}

	/**
	 * Reads configuration settings. The default path can be overridden
	 * by setting the ENV variable "NEPHELE_CONF_DIR".
	 * 
	 * @return Nephele's configuration
	 */
	protected Configuration getConfiguration() {
		String location = null;
		if (System.getenv(ENV_CONFIG_DIRECTORY) != null) {
			location = System.getenv(ENV_CONFIG_DIRECTORY);
		} else {
			location = DEFAULT_CONFIG_DIRECTORY;
		}

		GlobalConfiguration.loadConfiguration(location);
		Configuration config = GlobalConfiguration.getConfiguration();

		return config;
	}

	/**
	 * Submits the job based on the arguments
	 */
	public static void main(String[] args) throws ParseException {
		CliFrontend cli = new CliFrontend();
		cli.run(args);
	}
}
