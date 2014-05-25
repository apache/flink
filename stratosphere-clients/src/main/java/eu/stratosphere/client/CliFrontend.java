/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.client;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.commons.io.FileUtils;

import eu.stratosphere.api.common.JobExecutionResult;
import eu.stratosphere.api.common.accumulators.AccumulatorHelper;
import eu.stratosphere.client.program.Client;
import eu.stratosphere.client.program.PackagedProgram;
import eu.stratosphere.client.program.ProgramInvocationException;
import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.event.job.RecentJobEvent;
import eu.stratosphere.nephele.ipc.RPC;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobgraph.JobStatus;
import eu.stratosphere.nephele.net.NetUtils;
import eu.stratosphere.nephele.protocols.ExtendedManagementProtocol;
import eu.stratosphere.util.StringUtils;

/**
 * Implementation of a simple command line fronted for executing programs.
 */
public class CliFrontend {

	//actions
	private static final String ACTION_RUN = "run";
	private static final String ACTION_INFO = "info";
	private static final String ACTION_LIST = "list";
	private static final String ACTION_CANCEL = "cancel";
	
	// general options
	private static final Option HELP_OPTION = new Option("h", "help", false, "Show the help for the CLI Frontend.");
	private static final Option VERBOSE_OPTION = new Option("v", "verbose", false, "Print more detailed error messages.");
	
	// program (jar file) specific options
	private static final Option JAR_OPTION = new Option("j", "jarfile", true, "Stratosphere program JAR file.");
	private static final Option CLASS_OPTION = new Option("c", "class", true, "Class with the program entry point (\"main\" method or \"getPlan()\" method. Only needed if the JAR file does not specify the class in its manifest.");
	private static final Option PARALLELISM_OPTION = new Option("p", "parallelism", true, "The parallelism with which to run the program. Optional flag to override the default value specified in the configuration.");
	private static final Option ARGS_OPTION = new Option("a", "arguments", true, "Program arguments. Arguments can also be added without -a, simply as trailing parameters.");
	
	private static final Option ADDRESS_OPTION = new Option("m", "jobmanager", true, "Address of the JobManager (master) to which to connect. Use this flag to connect to a different JobManager than the one specified in the configuration.");
	
	// info specific options
	private static final Option DESCR_OPTION = new Option("d", "description", false, "Show description of expected program arguments");
	private static final Option PLAN_OPTION = new Option("e", "executionplan", false, "Show optimized execution plan of the program (JSON)");
	
	// list specific options
	private static final Option RUNNING_OPTION = new Option("r", "running", false, "Show running programs and their JobIDs");
	private static final Option SCHEDULED_OPTION = new Option("s", "scheduled", false, "Show scheduled prorgrams and their JobIDs");
	
	// canceling
	private static final Option ID_OPTION = new Option("i", "jobid", true, "JobID of program to cancel");
	
	static {
		initOptions();
	}
	
	// general options
	private static final Options GENRAL_OPTIONS = createGeneralOptions();
	
	// action options all include the general options
	private static final Options RUN_OPTIONS = getRunOptions(createGeneralOptions());
	private static final Options INFO_OPTIONS = getInfoOptions(createGeneralOptions());
	private static final Options LIST_OPTIONS = getListOptions(createGeneralOptions());
	private static final Options CANCEL_OPTIONS = getCancelOptions(createGeneralOptions());
	
	// config dir parameters
	private static final String ENV_CONFIG_DIRECTORY = "STRATOSPHERE_CONF_DIR";
	private static final String CONFIG_DIRECTORY_FALLBACK_1 = "../conf";
	private static final String CONFIG_DIRECTORY_FALLBACK_2 = "conf";
	
	public static final String JOBMANAGER_ADDRESS_FILE = ".yarn-jobmanager";
	

	private CommandLineParser parser;
	
	private boolean verbose;
	private boolean printHelp;
	
	private boolean globalConfigurationLoaded;

	/**
	 * Initializes the class
	 */
	public CliFrontend() {
		parser = new PosixParser();
	}
	
	// --------------------------------------------------------------------------------------------
	//  Setup of options
	// --------------------------------------------------------------------------------------------

	private static void initOptions() {
		HELP_OPTION.setRequired(false);
		VERBOSE_OPTION.setRequired(false);
		
		JAR_OPTION.setRequired(false);
		JAR_OPTION.setArgName("jarfile");
		
		CLASS_OPTION.setRequired(false);
		CLASS_OPTION.setArgName("classname");
		
		ADDRESS_OPTION.setRequired(false);
		ADDRESS_OPTION.setArgName("host:port");
		
		PARALLELISM_OPTION.setRequired(false);
		PARALLELISM_OPTION.setArgName("parallelism");
		
		ARGS_OPTION.setRequired(false);
		ARGS_OPTION.setArgName("programArgs");
		ARGS_OPTION.setArgs(Option.UNLIMITED_VALUES);
		
		PLAN_OPTION.setRequired(false);
		DESCR_OPTION.setRequired(false);
		
		RUNNING_OPTION.setRequired(false);
		SCHEDULED_OPTION.setRequired(false);
		
		ID_OPTION.setRequired(false);
		ID_OPTION.setArgName("jobID");
	}
	
	static Options createGeneralOptions() {
		Options options = new Options();
		options.addOption(HELP_OPTION);
		options.addOption(VERBOSE_OPTION);

		return options;
	}
	
	// gets the program options with the old flags for jar file and arguments
	static Options getProgramSpecificOptions(Options options) {
		options.addOption(JAR_OPTION);
		options.addOption(CLASS_OPTION);
		options.addOption(PARALLELISM_OPTION);
		options.addOption(ARGS_OPTION);
		return options;
	}
	
	// gets the program options without the old flags for jar file and arguments
	static Options getProgramSpecificOptionsWithoutDeprecatedOptions(Options options) {
		options.addOption(CLASS_OPTION);
		options.addOption(PARALLELISM_OPTION);
		return options;
	}
	
	/**
	 * Builds command line options for the run action.
	 * 
	 * @return Command line options for the run action.
	 */
	static Options getRunOptions(Options options) {
		Options o = getProgramSpecificOptions(options);
		return getJobManagerAddressOption(o);
	}
	
	static Options getRunOptionsWithoutDeprecatedOptions(Options options) {
		Options o = getProgramSpecificOptionsWithoutDeprecatedOptions(options);
		return getJobManagerAddressOption(o);
	}
	
	static Options getJobManagerAddressOption(Options options) {
		options.addOption(ADDRESS_OPTION);
		return options;
	}
	
	/**
	 * Builds command line options for the info action.
	 * 
	 * @return Command line options for the info action.
	 */
	static Options getInfoOptions(Options options) {
		options = getProgramSpecificOptions(options);
		options = getJobManagerAddressOption(options);
		options.addOption(DESCR_OPTION);
		options.addOption(PLAN_OPTION);
		return options;
	}
	
	static Options getInfoOptionsWithoutDeprecatedOptions(Options options) {
		options = getProgramSpecificOptionsWithoutDeprecatedOptions(options);
		options = getJobManagerAddressOption(options);
		options.addOption(DESCR_OPTION);
		options.addOption(PLAN_OPTION);
		return options;
	}
	
	/**
	 * Builds command line options for the list action.
	 * 
	 * @return Command line options for the list action.
	 */
	static Options getListOptions(Options options) {
		options.addOption(RUNNING_OPTION);
		options.addOption(SCHEDULED_OPTION);
		options = getJobManagerAddressOption(options);
		return options;
	}
	
	/**
	 * Builds command line options for the cancel action.
	 * 
	 * @return Command line options for the cancel action.
	 */
	static Options getCancelOptions(Options options) {
		options.addOption(ID_OPTION);
		options = getJobManagerAddressOption(options);
		return options;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Execute Actions
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Executions the run action.
	 * 
	 * @param args Command line arguments for the run action.
	 */
	protected int run(String[] args) {
		// Parse command line options
		CommandLine line;
		try {
			line = parser.parse(RUN_OPTIONS, args, false);
			evaluateGeneralOptions(line);
		}
		catch (MissingOptionException e) {
			System.out.println(e.getMessage());
			printHelpForRun();
			return 1;
		}
		catch (UnrecognizedOptionException e) {
			System.out.println(e.getMessage());
			printHelpForRun();
			return 2;
		}
		catch (Exception e) {
			return handleError(e);
		}
		
		// ------------ check for help first --------------
		
		if (printHelp) {
			printHelpForRun();
			return 0;
		}
		
		try {
			PackagedProgram program = buildProgram(line);
			if (program == null) {
				printHelpForRun();
				return 1;
			}
			
			Client client = getClient(line);
			if (client == null) {
				printHelpForRun();
				return 1;
			}
		
			int parallelism = -1;
			if (line.hasOption(PARALLELISM_OPTION.getOpt())) {
				String parString = line.getOptionValue(PARALLELISM_OPTION.getOpt());
				try {
					parallelism = Integer.parseInt(parString);
				} catch (NumberFormatException e) {
					System.out.println("The value " + parString + " is invalid for the degree of parallelism.");
					printHelpForRun();
					return 1;
				}
				
				if (parallelism <= 0) {
					System.out.println("Invalid value for the degree-of-parallelism. Parallelism must be greater than zero.");
					printHelpForRun();
					return 1;
				}
			}
		
			return executeProgram(program, client, parallelism);
		}
		catch (Throwable t) {
			return handleError(t);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	protected int executeProgram(PackagedProgram program, Client client, int parallelism) {
		JobExecutionResult execResult;
		try {
			client.setPrintStatusDuringExecution(true);
			execResult = client.run(program, parallelism, true);
		}
		catch (ProgramInvocationException e) {
			return handleError(e);
		}
		finally {
			program.deleteExtractedLibraries();
		}
		
		// we come here after the job has finished
		if (execResult != null) {
			System.out.println("Job Runtime: " + execResult.getNetRuntime());
			Map<String, Object> accumulatorsResult = execResult.getAllAccumulatorResults();
			if (accumulatorsResult.size() > 0) {
				System.out.println("Accumulator Results: ");
				System.out.println(AccumulatorHelper.getResultsFormated(accumulatorsResult));
			}
		}
		return 0;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Executes the info action.
	 * 
	 * @param args Command line arguments for the info action. 
	 */
	protected int info(String[] args) {
		// Parse command line options
		CommandLine line;
		try {
			line = parser.parse(INFO_OPTIONS, args, false);
			evaluateGeneralOptions(line);
		}
		catch (MissingOptionException e) {
			System.out.println(e.getMessage());
			printHelpForInfo();
			return 1;
		}
		catch (UnrecognizedOptionException e) {
			System.out.println(e.getMessage());
			printHelpForInfo();
			return 2;
		}
		catch (Exception e) {
			return handleError(e);
		}

		if (printHelp) {
			printHelpForInfo();
			return 0;
		}
		
		boolean description = line.hasOption(DESCR_OPTION.getOpt());
		boolean plan = line.hasOption(PLAN_OPTION.getOpt());
		
		if (!description && !plan) {
			System.out.println("ERROR: Specify the information to display.");
			printHelpForInfo();
			return 1;
		}

		// -------- build the packaged program -------------
		
		PackagedProgram program;
		try {
			program = buildProgram(line);
		} catch (Throwable t) {
			return handleError(t);
		}
		
		if (program == null) {
			printHelpForInfo();
			return 1;
		}
		
		int parallelism = -1;
		if (line.hasOption(PARALLELISM_OPTION.getOpt())) {
			String parString = line.getOptionValue(PARALLELISM_OPTION.getOpt());
			try {
				parallelism = Integer.parseInt(parString);
			} catch (NumberFormatException e) {
				System.out.println("The value " + parString + " is invalid for the degree of parallelism.");
				printHelpForRun();
				return 1;
			}
			
			if (parallelism <= 0) {
				System.out.println("Invalid value for the degree-of-parallelism. Parallelism must be greater than zero.");
				printHelpForRun();
				return 1;
			}
		}
		
		try {
			// check for description request
			if (description) {
				String descr = program.getDescription();
				
				if (descr != null) {
					System.out.println("-------------------- Program Description ---------------------");
					System.out.println(descr);
					System.out.println("--------------------------------------------------------------");
				} else {
					System.out.println("No description available for this program.");
				}
			}
			
			// check for json plan request
			if (plan) {
				Client client = getClient(line);
				String jsonPlan = client.getOptimizedPlanAsJson(program, parallelism);
				
				if (jsonPlan != null) {
					System.out.println("----------------------- Execution Plan -----------------------");
					System.out.println(jsonPlan);
					System.out.println("--------------------------------------------------------------");
				} else {
					System.out.println("JSON plan could not be compiled.");
				}
			}
			
			return 0;
		}
		catch (Throwable t) {
			return handleError(t);
		}
		finally {
			program.deleteExtractedLibraries();
		}
	}

	/**
	 * Executes the list action.
	 * 
	 * @param args Command line arguments for the list action.
	 */
	protected int list(String[] args) {
		// Parse command line options
		CommandLine line;
		try {
			line = parser.parse(LIST_OPTIONS, args, false);
		}
		catch (MissingOptionException e) {
			System.out.println(e.getMessage());
			printHelpForList();
			return 1;
		}
		catch (UnrecognizedOptionException e) {
			System.out.println(e.getMessage());
			printHelpForList();
			return 2;
		}
		catch (Exception e) {
			return handleError(e);
		}
		
		if (printHelp) {
			printHelpForList();
			return 0;
		}
		
		// get list options
		boolean running = line.hasOption(RUNNING_OPTION.getOpt());
		boolean scheduled = line.hasOption(SCHEDULED_OPTION.getOpt());
		
		if (!running && !scheduled) {
			System.out.println("Error: Specify the status of the jobs to list.");
			printHelpForList();
			return 1;
		}
		
		ExtendedManagementProtocol jmConn = null;
		try {
			jmConn = getJobManagerConnection(line);
			if (jmConn == null) {
				printHelpForList();
				return 1;
			}
			
			List<RecentJobEvent> recentJobs = jmConn.getRecentJobs();
			
			ArrayList<RecentJobEvent> runningJobs = null;
			ArrayList<RecentJobEvent> scheduledJobs = null;
			if (running) {
				runningJobs = new ArrayList<RecentJobEvent>();
			}
			if (scheduled) {
				scheduledJobs = new ArrayList<RecentJobEvent>();
			}
			
			for (RecentJobEvent rje : recentJobs) {
				
				if (running && rje.getJobStatus().equals(JobStatus.RUNNING)) {
					runningJobs.add(rje);
				}
				if (scheduled && rje.getJobStatus().equals(JobStatus.SCHEDULED)) {
					scheduledJobs.add(rje);
				}
			}
			
			SimpleDateFormat df = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
			Comparator<RecentJobEvent> njec = new Comparator<RecentJobEvent>(){
				
				@Override
				public int compare(RecentJobEvent o1, RecentJobEvent o2) {
					return (int)(o1.getTimestamp()-o2.getTimestamp());
				}
			};
			
			if (running) {
				if(runningJobs.size() == 0) {
					System.out.println("No running jobs.");
				} else {
					Collections.sort(runningJobs, njec);
					
					System.out.println("------------------------ Running Jobs ------------------------");
					for(RecentJobEvent je : runningJobs) {
						System.out.println(df.format(new Date(je.getTimestamp()))+" : "+je.getJobID().toString()+" : "+je.getJobName());
					}
					System.out.println("--------------------------------------------------------------");
				}
			}
			if (scheduled) {
				if(scheduledJobs.size() == 0) {
					System.out.println("No scheduled jobs.");
				} else {
					Collections.sort(scheduledJobs, njec);
					
					System.out.println("----------------------- Scheduled Jobs -----------------------");
					for(RecentJobEvent je : scheduledJobs) {
						System.out.println(df.format(new Date(je.getTimestamp()))+" : "+je.getJobID().toString()+" : "+je.getJobName());
					}
					System.out.println("--------------------------------------------------------------");
				}
			}
			return 0;
		}
		catch (Throwable t) {
			return handleError(t);
		}
		finally {
			if (jmConn != null) {
				try {
					RPC.stopProxy(jmConn);
				} catch (Throwable t) {
					System.out.println("Could not cleanly shut down connection from compiler to job manager");
				}
			}
			jmConn = null;
		}
		
	}
	
	/**
	 * Executes the cancel action.
	 * 
	 * @param args Command line arguments for the cancel action.
	 */
	protected int cancel(String[] args) {
		// Parse command line options
		CommandLine line;
		try {
			line = parser.parse(CANCEL_OPTIONS, args, false);
		}
		catch (MissingOptionException e) {
			System.out.println(e.getMessage());
			printHelpForCancel();
			return 1;
		}
		catch (UnrecognizedOptionException e) {
			System.out.println(e.getMessage());
			printHelpForCancel();
			return 2;
		}
		catch (Exception e) {
			return handleError(e);
		}
		
		if (printHelp) {
			printHelpForCancel();
			return 0;
		}
		
		JobID jobId;
		
		if (line.hasOption(ID_OPTION.getOpt())) {
			String jobIdString = line.getOptionValue(ID_OPTION.getOpt());
			try {
				jobId = new JobID(StringUtils.hexStringToByte(jobIdString));
			} catch (Exception e) {
				System.out.println("Error: The value for the Job ID is not a valid ID.");
				printHelpForCancel();
				return 1;
			}
		} else {
			System.out.println("Error: Specify a Job ID to cancel a job.");
			printHelpForCancel();
			return 1;
		}
		
		ExtendedManagementProtocol jmConn = null;
		try {
			jmConn = getJobManagerConnection(line);
			if (jmConn == null) {
				printHelpForCancel();
				return 1;
			}
			
			jmConn.cancelJob(jobId);
			return 0;
		}
		catch (Throwable t) {
			return handleError(t);
		}
		finally {
			if (jmConn != null) {
				try {
					RPC.stopProxy(jmConn);
				} catch (Throwable t) {
					System.out.println("Warning: Could not cleanly shut down connection to the JobManager.");
				}
			}
			jmConn = null;
		}
	}

	/**
	 * @param line
	 * 
	 * @return Either a PackagedProgram (upon success), or null;
	 */
	protected PackagedProgram buildProgram(CommandLine line) {
		String[] programArgs = line.hasOption(ARGS_OPTION.getOpt()) ?
				line.getOptionValues(ARGS_OPTION.getOpt()) :
				line.getArgs();
	
		// take the jar file from the option, or as the first trailing parameter (if available)
		String jarFilePath = null;
		if (line.hasOption(JAR_OPTION.getOpt())) {
			jarFilePath = line.getOptionValue(JAR_OPTION.getOpt());
		}
		else if (programArgs.length > 0) {
			jarFilePath = programArgs[0];
			programArgs = Arrays.copyOfRange(programArgs, 1, programArgs.length);
		}
		else {
			System.out.println("Error: Jar file is not set.");
			return null;
		}
		
		File jarFile = new File(jarFilePath);
		
		// Check if JAR file exists
		if (!jarFile.exists()) {
			System.out.println("Error: Jar file does not exist.");
			return null;
		}
		else if (!jarFile.isFile()) {
			System.out.println("Error: Jar file is not a file.");
			return null;
		}
		
		// Get assembler class
		String entryPointClass = line.hasOption(CLASS_OPTION.getOpt()) ?
				line.getOptionValue(CLASS_OPTION.getOpt()) :
				null;
				
		try {
			return entryPointClass == null ? 
					new PackagedProgram(jarFile, programArgs) :
					new PackagedProgram(jarFile, entryPointClass, programArgs);
		} catch (ProgramInvocationException e) {
			handleError(e);
			return null;
		}
	}
	
	protected InetSocketAddress getJobManagerAddress(CommandLine line) throws IOException {
		Configuration configuration = getGlobalConfiguration();
		
		// first, check if the address comes from the command line option
		if (line.hasOption(ADDRESS_OPTION.getOpt())) {
			try {
				String address = line.getOptionValue(ADDRESS_OPTION.getOpt());
				return RemoteExecutor.getInetFromHostport(address);
			}
			catch (Exception e) {
				System.out.println("Error: The JobManager address has an invalid format. " + e.getMessage());
				return null;
			}
		}
		else {
			// second, search for a .yarn-jobmanager file
			String loc = getConfigurationDirectory();
			File jmAddressFile = new File(loc + '/' + JOBMANAGER_ADDRESS_FILE);
			
			if (jmAddressFile.exists()) {
				try {
					String address = FileUtils.readFileToString(jmAddressFile).trim();
					System.out.println("Found a " + JOBMANAGER_ADDRESS_FILE + " file, using \""+address+"\" to connect to the JobManager");
					
					return RemoteExecutor.getInetFromHostport(address);
				}
				catch (Exception e) {
					System.out.println("Found a " + JOBMANAGER_ADDRESS_FILE + " file, but could not read the JobManager address from the file. " 
								+ e.getMessage());
					return null;
				}
			}
			else {
				// regular config file gives the address
				String jobManagerAddress = configuration.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);
				
				// verify that there is a jobmanager address and port in the configuration
				if (jobManagerAddress == null) {
					System.out.println("Error: Found no configuration in the config directory '" + 
							getConfigurationDirectory() + "' that specifies the JobManager address.");
					return null;
				}
				
				int jobManagerPort;
				try {
					jobManagerPort = configuration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, -1);
				} catch (NumberFormatException e) {
					System.out.println("Invalid value for the JobManager IPC port (" + ConfigConstants.JOB_MANAGER_IPC_PORT_KEY +
							") in the configuration.");
					return null;
				}
				
				if (jobManagerPort == -1) {
					System.out.println("Error: Found no configuration in the config directory '" + 
							getConfigurationDirectory() + "' that specifies the JobManager port.");
					return null;
				}
				
				return new InetSocketAddress(jobManagerAddress, jobManagerPort);
			}
		}
	}
	
	protected ExtendedManagementProtocol getJobManagerConnection(CommandLine line) throws IOException {
		InetSocketAddress jobManagerAddress = getJobManagerAddress(line);
		if (jobManagerAddress == null) {
			return null;
		}
		
		String address = jobManagerAddress.getAddress().getHostAddress();
		int port = jobManagerAddress.getPort();
		
		return RPC.getProxy(ExtendedManagementProtocol.class, 
				new InetSocketAddress(address, port), NetUtils.getSocketFactory());
	}
	
	
	protected String getConfigurationDirectory() {
		String location = null;
		if (System.getenv(ENV_CONFIG_DIRECTORY) != null) {
			location = System.getenv(ENV_CONFIG_DIRECTORY);
		} else if (new File(CONFIG_DIRECTORY_FALLBACK_1).exists()) {
			location = CONFIG_DIRECTORY_FALLBACK_1;
		} else if (new File(CONFIG_DIRECTORY_FALLBACK_2).exists()) {
			location = CONFIG_DIRECTORY_FALLBACK_2;
		} else {
			throw new RuntimeException("The configuration directory was not found. Please configure the '" + 
					ENV_CONFIG_DIRECTORY + "' environment variable properly.");
		}
		return location;
	}
	/**
	 * Reads configuration settings. The default path can be overridden
	 * by setting the ENV variable "STRATOSPHERE_CONF_DIR".
	 * 
	 * @return Stratosphere's global configuration
	 */
	protected Configuration getGlobalConfiguration() {
		if (!globalConfigurationLoaded) {
			String location = getConfigurationDirectory();
			GlobalConfiguration.loadConfiguration(location);
			globalConfigurationLoaded = true;
		}
		return GlobalConfiguration.getConfiguration();
	}
	
	protected Client getClient(CommandLine line) throws IOException {
		return new Client(getJobManagerAddress(line), getGlobalConfiguration());
	}

	/**
	 * Prints the help for the client.
	 * 
	 * @param options A map with options for actions. 
	 */
	private void printHelp() {
		System.out.println("./stratosphere <ACTION> [GENERAL_OPTIONS] [ARGUMENTS]");
		
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(80);
		formatter.setLeftPadding(5);
		formatter.setSyntaxPrefix("  general options:");
		formatter.printHelp(" ", GENRAL_OPTIONS);
		
		printHelpForRun();
		printHelpForInfo();
		printHelpForList();
		printHelpForCancel();
	}
	
	private void printHelpForRun() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setLeftPadding(5);
		formatter.setWidth(80);
		
		System.out.println("\nAction \"run\" compiles and runs a program.");
		System.out.println("\n  Syntax: run [OPTIONS] <jar-file> <arguments>");
		formatter.setSyntaxPrefix("  \"run\" action arguments:");
		formatter.printHelp(" ", getRunOptionsWithoutDeprecatedOptions(new Options()));
	}
	
	private void printHelpForInfo() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setLeftPadding(5);
		formatter.setWidth(80);
		
		System.out.println("\nAction \"info\" displays information about a program.");
		formatter.setSyntaxPrefix("  \"info\" action arguments:");
		formatter.printHelp(" ", getInfoOptionsWithoutDeprecatedOptions(new Options()));
	}
	
	private void printHelpForList() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setLeftPadding(5);
		formatter.setWidth(80);
		
		System.out.println("\nAction \"list\" lists running and finished programs.");
		formatter.setSyntaxPrefix("  \"list\" action arguments:");
		formatter.printHelp(" ", getListOptions(new Options()));
	}
	
	private void printHelpForCancel() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setLeftPadding(5);
		formatter.setWidth(80);
		
		System.out.println("\nAction \"cancel\" cancels a running program.");
		formatter.setSyntaxPrefix("  \"cancel\" action arguments:");
		formatter.printHelp(" ", getCancelOptions(new Options()));
	}
	
	
	/**
	 * Displays exceptions.
	 * 
	 * @param e the exception to display.
	 */
	private int handleError(Throwable t) {
		System.out.println("Error: " + t.getMessage());
		if (this.verbose) {
			t.printStackTrace();
		} else {
			System.out.println("For a more detailed error message use the vebose output option '-v'.");
		}
		return 1;
	}


	

	private void evaluateGeneralOptions(CommandLine line) {
		// check help flag
		this.printHelp = line.hasOption(HELP_OPTION.getOpt());
		
		// check verbosity flag
		this.verbose = line.hasOption(VERBOSE_OPTION.getOpt());
	}
	
	/**
	 * Parses the command line arguments and starts the requested action.
	 * 
	 * @param args command line arguments of the client.
	 * @return The return code of the program
	 */
	public int parseParameters(String[] args) {
		
		// check for action
		if (args.length < 1) {
			System.out.println("Please specify an action.");
			printHelp();
			return 1;
		}
		
		// get action
		String action = args[0];
		
		// remove action from parameters
		String[] params = Arrays.copyOfRange(args, 1, args.length);
		
		// do action
		if (action.equals(ACTION_RUN)) {
			return run(params);
		} else if (action.equals(ACTION_LIST)) {
			return list(params);
		} else if (action.equals(ACTION_INFO)) {
			return info(params);
		} else if (action.equals(ACTION_CANCEL)) {
			return cancel(params);
		} else if (action.equals("-h") || action.equals("--help")) {
			printHelp();
			return 0;
		} else {
			System.out.println("Invalid action!");
			printHelp();
			return 1;
		}
	}

	

	/**
	 * Submits the job based on the arguments
	 */
	public static void main(String[] args) throws ParseException {
		CliFrontend cli = new CliFrontend();
		int retCode = cli.parseParameters(args);
		System.exit(retCode);
	}
}
