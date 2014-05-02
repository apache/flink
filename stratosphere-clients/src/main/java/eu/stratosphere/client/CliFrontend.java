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
import eu.stratosphere.compiler.CompilerException;
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
 * Implementation of a simple command line fronted for executing PACT programs.
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
	private static final Option JAR_OPTION = new Option("j", "jarfile", true, "Stratosphere program JAR file");
	private static final Option CLASS_OPTION = new Option("c", "class", true, "Program class");
	private static final Option ADDRESS_OPTION = new Option("m", "jobmanager", true, "Jobmanager to which the program is submitted");
	private static final Option ARGS_OPTION = new Option("a", "arguments", true, "Program arguments. Arguments can also be added without -a, simply as trailing parameters.");
	
	// run specific options
	private static final Option WAIT_OPTION = new Option("w", "wait", false, "Wait for program to finish");
	
	// info specific options
	private static final Option DESCR_OPTION = new Option("d", "description", false, "Show description of expected program arguments");
	private static final Option PLAN_OPTION = new Option("p", "plan", false, "Show optimized execution plan of the program (JSON)");
	
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
		
		ARGS_OPTION.setRequired(false);
		ARGS_OPTION.setArgName("programArgs");
		ARGS_OPTION.setArgs(Option.UNLIMITED_VALUES);
		
		WAIT_OPTION.setRequired(false);
		
		PLAN_OPTION.setRequired(false);
		DESCR_OPTION.setRequired(false);
		
		RUNNING_OPTION.setRequired(false);
		SCHEDULED_OPTION.setRequired(false);
		
		ID_OPTION.setRequired(false);
		ID_OPTION.setArgName("jobID");
	}
	
	private static Options createGeneralOptions() {
		Options options = new Options();
		options.addOption(HELP_OPTION);
		options.addOption(VERBOSE_OPTION);

		return options;
	}
	
	private static Options getProgramSpecificOptions(Options options) {
		options.addOption(JAR_OPTION);
		options.addOption(CLASS_OPTION);
		options.addOption(ADDRESS_OPTION);
		options.addOption(ARGS_OPTION);
		return options;
	}
	
	/**
	 * Builds command line options for the run action.
	 * 
	 * @return Command line options for the run action.
	 */
	private static Options getRunOptions(Options options) {
		options = getProgramSpecificOptions(options);
		options.addOption(WAIT_OPTION);
		return options;
	}
	
	/**
	 * Builds command line options for the info action.
	 * 
	 * @return Command line options for the info action.
	 */
	private static Options getInfoOptions(Options options) {
		options = getProgramSpecificOptions(options);
		options.addOption(DESCR_OPTION);
		options.addOption(PLAN_OPTION);
		return options;
	}
	
	/**
	 * Builds command line options for the list action.
	 * 
	 * @return Command line options for the list action.
	 */
	private static Options getListOptions(Options options) {
		options.addOption(RUNNING_OPTION);
		options.addOption(SCHEDULED_OPTION);
		return options;
	}
	
	/**
	 * Builds command line options for the cancel action.
	 * 
	 * @return Command line options for the cancel action.
	 */
	private static Options getCancelOptions(Options options) {
		options.addOption(ID_OPTION);
		return options;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Execut Actions
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Executions the run action.
	 * 
	 * @param args Command line arguments for the run action.
	 */
	private int run(String[] args) {
		
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
		
		if (printHelp) {
			printHelpForRun();
			return 0;
		}
		
		File jarFile = null;
		String entryPointClass = null;
		String[] programArgs = null;
		String address = null;
		boolean wait = false;
		
		if (line.hasOption(ADDRESS_OPTION.getOpt())) {
			address = line.getOptionValue(ADDRESS_OPTION.getOpt());
		}
		
		// Get jar file
		if (line.hasOption(JAR_OPTION.getOpt())) {
			String jarFilePath = line.getOptionValue(JAR_OPTION.getOpt());
			jarFile = new File(jarFilePath);
			
			// Check if JAR file exists
			if (!jarFile.exists()) {
				System.out.println("Error: Jar file does not exist.");
				printHelpForRun();
				return 1;
			}
			else if (!jarFile.isFile()) {
				System.out.println("Error: Jar file is not a file.");
				printHelpForRun();
				return 1;
			}
		} else {
			System.out.println("Error: Jar file is not set.");	
			printHelpForRun();
			return 1;
		}
		
		// Get assembler class
		if (line.hasOption(CLASS_OPTION.getOpt())) {
			entryPointClass = line.getOptionValue(CLASS_OPTION.getOpt());
		}
		
		// get program arguments
		if (line.hasOption(ARGS_OPTION.getOpt())) {
			programArgs = line.getOptionValues(ARGS_OPTION.getOpt());
		} else {
			programArgs = line.getArgs();
		}
		
		// see if there is a file containing the jobManager address.
		String loc = getConfigurationDirectory();
		File jmAddressFile = new File(loc + "/" + JOBMANAGER_ADDRESS_FILE);
		boolean yarnMode = false;
		if (jmAddressFile.exists()) {
			try {
				address = FileUtils.readFileToString(jmAddressFile).trim();
				System.out.println("Found a " + JOBMANAGER_ADDRESS_FILE + " file, using \""+address+"\" to connect to the JobManager");
				yarnMode = true;
			} catch (IOException e) {}
		}
		
		// get wait flag
		wait = line.hasOption(WAIT_OPTION.getOpt());
		
		// Try to get load plan
		PackagedProgram program;
		try {
			if (entryPointClass == null) {
				program = new PackagedProgram(jarFile, programArgs);
			} else {
				program = new PackagedProgram(jarFile, entryPointClass, programArgs);
			}
		} catch (ProgramInvocationException e) {
			return handleError(e);
		}

		Configuration configuration = getConfiguration();
		Client client;
		InetSocketAddress socket = null;
		if (address != null && !address.isEmpty()) {
			socket = RemoteExecutor.getInetFromHostport(address);
			client = new Client(socket, configuration);
		} else {
			client = new Client(configuration);
		}
		client.setPrintStatusDuringExecution(true);
		
		
		JobExecutionResult execResult;
		try {
			execResult = client.run(program, wait);
		}
		catch (ProgramInvocationException e) {
			return handleError(e);
		}
		finally {
			program.deleteExtractedLibraries();
		}
		
		if (wait && execResult != null) {
			System.out.println("Job Runtime: " + execResult.getNetRuntime());
			Map<String, Object> accumulatorsResult = execResult.getAllAccumulatorResults();
			if (accumulatorsResult.size() > 0) {
				System.out.println("Accumulator Results: ");
				System.out.println(AccumulatorHelper.getResultsFormated(accumulatorsResult));
			}
		}
		else {
			if(!yarnMode) {
				if (address != null && !address.isEmpty()) {
					System.out.println("Job successfully submitted. Use -w (or --wait) option to track the progress here.\n"
							+ "JobManager web interface: http://"
							+ socket.getHostName()
							+ ":" + configuration.getInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, ConfigConstants.DEFAULT_JOB_MANAGER_WEB_FRONTEND_PORT));
				} else {
					System.out.println("Job successfully submitted. Use -w (or --wait) option to track the progress here.\n"
						+ "JobManager web interface: http://"
						+ configuration.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null)
						+ ":" + configuration.getInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, ConfigConstants.DEFAULT_JOB_MANAGER_WEB_FRONTEND_PORT));
				}
			} else {
				System.out.println("Job successfully submitted. Use -w (or --wait) option to track the progress here.\n");
			}
		}
		return 0;
	}
	
	/**
	 * Executes the info action.
	 * 
	 * @param args Command line arguments for the info action. 
	 */
	private int info(String[] args) {
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
		
		File jarFile = null;
		String assemblerClass = null;
		String[] programArgs = null;
		
		boolean description;
		boolean plan;
		
		// Get jar file
		if (line.hasOption(JAR_OPTION.getOpt())) {
			jarFile = new File(line.getOptionValue(JAR_OPTION.getOpt()));
			
			// Check if JAR file exists
			if (!jarFile.exists()) {
				System.out.println("Error: Jar file does not exist.");
				printHelpForInfo();
				return 1;
			}
			else if (!jarFile.isFile()) {
				System.out.println("Error: Jar file is not a file.");
				printHelpForInfo();
				return 1;
			}
		} else {
			System.out.println("Error: Jar file is not set.");
			printHelpForInfo();
			return 1;
		}
		
		// Get assembler class
		if (line.hasOption(CLASS_OPTION.getOpt())) {
			assemblerClass = line.getOptionValue(CLASS_OPTION.getOpt());
		}
		
		// get program arguments
		if (line.hasOption(ARGS_OPTION.getOpt())) {
			programArgs = line.getOptionValues(ARGS_OPTION.getOpt());
		} else {
			programArgs = line.getArgs();
		}
		
		description = line.hasOption(DESCR_OPTION.getOpt());
		plan = line.hasOption(PLAN_OPTION.getOpt());
		
		if (!description && !plan) {
			System.out.println("ERROR: Specify the information to display.");
			printHelpForInfo();
			return 1;
		}
		
		// Try to get load plan
		PackagedProgram program;
		try {
			if (assemblerClass == null) {
				program = new PackagedProgram(jarFile, programArgs);
			} else {
				program = new PackagedProgram(jarFile, assemblerClass, programArgs);
			}
		} catch (ProgramInvocationException e) {
			return handleError(e);
		}
		
		try {
			// check for description request
			if (description) {
				String descr = null;
				try {
					descr = program.getDescription();
				} catch (Exception e) {
					return handleError(e);
				}
				
				if (descr != null) {
					System.out.println("-------------------- Program Description ---------------------");
					System.out.println(descr);
					System.out.println("--------------------------------------------------------------");
				} else {
					System.out.println("No description available for this plan.");
				}
			}
			
			// check for json plan request
			if (plan) {
				String jsonPlan = null;
				
				Configuration configuration = getConfiguration();
				Client client = new Client(configuration);
				try {
					jsonPlan = client.getOptimizedPlanAsJson(program);
				}
				catch (ProgramInvocationException e) {
					return handleError(e);
				}
				catch (CompilerException e) {
					return handleError(e);
				}
				
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
		finally {
			program.deleteExtractedLibraries();
		}
	}

	/**
	 * Executes the list action.
	 * 
	 * @param args Command line arguments for the list action.
	 */
	private int list(String[] args) {
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
			
			jmConn = getJMConnection();
			List<RecentJobEvent> recentJobs = jmConn.getRecentJobs();
			
			ArrayList<RecentJobEvent> runningJobs = null;
			ArrayList<RecentJobEvent> scheduledJobs = null;
			if(running) {
				runningJobs = new ArrayList<RecentJobEvent>();
			}
			if(scheduled) {
				scheduledJobs = new ArrayList<RecentJobEvent>();
			}
			
			for(RecentJobEvent rje : recentJobs) {
				
				if(running && rje.getJobStatus().equals(JobStatus.RUNNING)) {
					runningJobs.add(rje);
				}
				if(scheduled && rje.getJobStatus().equals(JobStatus.SCHEDULED)) {
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
	private int cancel(String[] args) {
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
		
		String jobId = null;
		
		if (line.hasOption(ID_OPTION.getOpt())) {
			jobId = line.getOptionValue(ID_OPTION.getOpt());
		} else {
			System.out.println("Error: Specify a jobID to cancel a job.");
			printHelpForCancel();
			return 1;
		}
		
		ExtendedManagementProtocol jmConn = null;
		try {
			
			jmConn = getJMConnection();
			jmConn.cancelJob(new JobID(StringUtils.hexStringToByte(jobId)));
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
	 * Sets up a connection to the JobManager.
	 * 
	 * @return Connection to the JobManager.
	 * @throws IOException
	 */
	private ExtendedManagementProtocol getJMConnection() throws IOException {
		Configuration config = getConfiguration();
		String jmHost = config.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);
		String jmPort = config.getString(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, null);
		
		if(jmHost == null) {
			handleError(new Exception("JobManager address could not be determined."));
		}
		if(jmPort == null) {
			handleError(new Exception("JobManager port could not be determined."));
		}
		
		return RPC.getProxy(ExtendedManagementProtocol.class, 
			new InetSocketAddress(jmHost, Integer.parseInt(jmPort)), NetUtils.getSocketFactory());
	}
	

	/**
	 * Prints the help for the client.
	 * 
	 * @param options A map with options for actions. 
	 */
	private void printHelp() {
		System.out.println("./stratosphere [ACTION] [GENERAL_OPTIONS] [ACTION_ARGUMENTS]");
		
		HelpFormatter formatter = new HelpFormatter();
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
		
		System.out.println("\nAction \"run\" compiles and submits a Stratosphere program.");
		formatter.setSyntaxPrefix("  \"run\" action arguments:");
		formatter.printHelp(" ", getRunOptions(new Options()));
	}
	
	private void printHelpForInfo() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setLeftPadding(5);
		
		System.out.println("\nAction \"info\" displays information about a Stratosphere program.");
		formatter.setSyntaxPrefix("  \"info\" action arguments:");
		formatter.printHelp(" ", getInfoOptions(new Options()));
	}
	
	private void printHelpForList() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setLeftPadding(5);
		
		System.out.println("\nAction \"list\" lists submitted Stratosphere programs.");
		formatter.setSyntaxPrefix("  \"list\" action arguments:");
		formatter.printHelp(" ", getListOptions(new Options()));
	}
	
	private void printHelpForCancel() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setLeftPadding(5);
		
		System.out.println("\nAction \"cancel\" cancels a submitted Stratosphere program.");
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
			System.out.println("For a more detailed error message use the '-v' option");
		}
		return 1;
	}

	private String getConfigurationDirectory() {
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
	private Configuration getConfiguration() {
		final String location = getConfigurationDirectory();
		GlobalConfiguration.loadConfiguration(location);
		Configuration config = GlobalConfiguration.getConfiguration();

		return config;
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
	 */
	private void parseParameters(String[] args) {
		
		// check for action
		if (args.length < 1) {
			System.out.println("Please specify an action.");
			printHelp();
			System.exit(1);
		}
		
		// get action
		String action = args[0];
		
		// remove action from parameters
		String[] params = Arrays.copyOfRange(args, 1, args.length);
		
		int returnCode;
		
		// do action
		if (action.equals(ACTION_RUN)) {
			returnCode = run(params);
		} else if (action.equals(ACTION_LIST)) {
			returnCode = list(params);
		} else if (action.equals(ACTION_INFO)) {
			returnCode = info(params);
		} else if (action.equals(ACTION_CANCEL)) {
			returnCode = cancel(params);
		} else if (action.equals("-h") || action.equals("--help")) {
			printHelp();
			returnCode = 0;
		} else {
			System.out.println("Invalid action!");
			printHelp();
			returnCode = 1;
		}
		
		System.exit(returnCode);
	}

	

	/**
	 * Submits the job based on the arguments
	 */
	public static void main(String[] args) throws ParseException {
		CliFrontend cli = new CliFrontend();
		cli.parseParameters(args);
	}
}
