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


package org.apache.flink.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.Client;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.yarn.AbstractFlinkYarnClient;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.messages.JobManagerMessages.CancelJob;
import org.apache.flink.runtime.messages.JobManagerMessages.RequestRunningJobs$;
import org.apache.flink.runtime.messages.JobManagerMessages.RunningJobs;
import org.apache.flink.runtime.yarn.AbstractFlinkYarnCluster;
import org.apache.flink.runtime.yarn.FlinkYarnClusterStatus;
import org.apache.flink.util.StringUtils;
import scala.concurrent.duration.FiniteDuration;

/**
 * Implementation of a simple command line fronted for executing programs.
 */
public class CliFrontend {

	// run job by deploying Flink into a YARN cluster, if this string is specified as the jobmanager address
	public static final String YARN_DEPLOY_JOBMANAGER = "yarn-cluster";

	// command line interface of the YARN session, with a special initialization here to prefix all options with y/yarn.
	private static FlinkYarnSessionCli yarnSessionCLi = new FlinkYarnSessionCli("y", "yarn");

	//actions
	private static final String ACTION_RUN = "run";
	private static final String ACTION_INFO = "info";
	private static final String ACTION_LIST = "list";
	private static final String ACTION_CANCEL = "cancel";

	// general options
	private static final Option HELP_OPTION = new Option("h", "help", false, "Show the help for the CLI Frontend.");
	private static final Option VERBOSE_OPTION = new Option("v", "verbose", false, "Print more detailed error messages.");

	// program (jar file) specific options
	private static final Option JAR_OPTION = new Option("j", "jarfile", true, "Flink program JAR file.");
	private static final Option CLASS_OPTION = new Option("c", "class", true, "Class with the program entry point (\"main\" method or \"getPlan()\" method. Only needed if the JAR file does not specify the class in its manifest.");
	private static final Option PARALLELISM_OPTION = new Option("p", "parallelism", true, "The parallelism with which to run the program. Optional flag to override the default value specified in the configuration.");
	private static final Option ARGS_OPTION = new Option("a", "arguments", true, "Program arguments. Arguments can also be added without -a, simply as trailing parameters.");

	private static final Option ADDRESS_OPTION = new Option("m", "jobmanager", true, "Address of the JobManager (master) to which to connect. Specify '"+YARN_DEPLOY_JOBMANAGER+"' as the JobManager to deploy a YARN cluster for the job. Use this flag to connect to a different JobManager than the one specified in the configuration.");

	// info specific options
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
	private static final Options GENERAL_OPTIONS = createGeneralOptions();
	
	// action options all include the general options
	private static final Options RUN_OPTIONS = getRunOptions(createGeneralOptions());
	private static final Options INFO_OPTIONS = getInfoOptions(createGeneralOptions());
	private static final Options LIST_OPTIONS = getListOptions(createGeneralOptions());
	private static final Options CANCEL_OPTIONS = getCancelOptions(createGeneralOptions());
	
	// config dir parameters
	private static final String ENV_CONFIG_DIRECTORY = "FLINK_CONF_DIR";
	private static final String CONFIG_DIRECTORY_FALLBACK_1 = "../conf";
	private static final String CONFIG_DIRECTORY_FALLBACK_2 = "conf";
	
	/**
	 * YARN-session related constants
	 */
	public static final String YARN_PROPERTIES_FILE = ".yarn-properties";
	public static final String YARN_PROPERTIES_JOBMANAGER_KEY = "jobManager";
	public static final String YARN_PROPERTIES_DOP = "degreeOfParallelism";
	public static final String YARN_PROPERTIES_DYNAMIC_PROPERTIES_STRING = "dynamicPropertiesString";
	// this has to be a regex for String.split()
	public static final String YARN_DYNAMIC_PROPERTIES_SEPARATOR = "@@";
	

	private CommandLineParser parser;
	
	private boolean verbose;
	private boolean printHelp;
	
	private boolean globalConfigurationLoaded;
	
	private boolean yarnPropertiesLoaded = false;
	
	private Properties yarnProperties;

	// this flag indicates if the given Job is executed using a YARN cluster,
	// started for this purpose.
	private boolean runInYarnCluster = false;

	private AbstractFlinkYarnCluster yarnCluster = null;

	protected String configurationDirectory = null;


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

		// also add the YARN options so that the parser can parse them
		yarnSessionCLi.getYARNSessionCLIOptions(options);
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
		options.addOption(PLAN_OPTION);
		return options;
	}
	
	static Options getInfoOptionsWithoutDeprecatedOptions(Options options) {
		options = getProgramSpecificOptionsWithoutDeprecatedOptions(options);
		options = getJobManagerAddressOption(options);
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
			line = parser.parse(RUN_OPTIONS, args, true);
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
			
			Client client = getClient(line, program.getUserCodeClassLoader(), program.getMainClassName());
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
			int programResult = executeProgram(program, client, parallelism);
			// check if the program has been executed in a "job only" YARN cluster.
			if(runInYarnCluster) {
				List<String> msgs = yarnCluster.getNewMessages();
				if(msgs != null && msgs.size() > 1) {
					System.out.println("The following messages were created by the YARN cluster while running the Job:");
					for(String msg : msgs) {
						System.out.println(msg);
					}
				}
				if(yarnCluster.hasFailed()) {
					System.out.println("YARN cluster is in failed state!");
					System.out.println("YARN Diagnostics: " + yarnCluster.getDiagnostics());
				}
				System.out.println("Shutting down YARN cluster");
				yarnCluster.shutdown();
			}

			return programResult;
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
		
		boolean plan = line.hasOption(PLAN_OPTION.getOpt());
		
		if (!plan) {
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
			// check for json plan request
			if (plan) {
				Client client = getClient(line, program.getUserCodeClassLoader(), program.getMainClassName());
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
		
		try {
			ActorRef jobManager = getJobManager(line);
			if (jobManager == null) {
				printHelpForList();
				return 1;
			}

			Iterable<ExecutionGraph> jobs = AkkaUtils.<RunningJobs>ask(jobManager,
					RequestRunningJobs$.MODULE$, getAkkaTimeout()).asJavaIterable();

			ArrayList<ExecutionGraph> runningJobs = null;
			ArrayList<ExecutionGraph> scheduledJobs = null;
			if (running) {
				runningJobs = new ArrayList<ExecutionGraph>();
			}
			if (scheduled) {
				scheduledJobs = new ArrayList<ExecutionGraph>();
			}
			
			for (ExecutionGraph rj : jobs) {
				
				if (running && rj.getState().equals(JobStatus.RUNNING)) {
					runningJobs.add(rj);
				}
				if (scheduled && rj.getState().equals(JobStatus.CREATED)) {
					scheduledJobs.add(rj);
				}
			}
			
			SimpleDateFormat df = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
			Comparator<ExecutionGraph> njec = new Comparator<ExecutionGraph>(){
				
				@Override
				public int compare(ExecutionGraph o1, ExecutionGraph o2) {
					return (int)(o1.getStatusTimestamp(o1.getState())-o2.getStatusTimestamp(o2
							.getState()));
				}
			};
			
			if (running) {
				if(runningJobs.size() == 0) {
					System.out.println("No running jobs.");
				} else {
					Collections.sort(runningJobs, njec);
					
					System.out.println("------------------------ Running Jobs ------------------------");
					for(ExecutionGraph rj : runningJobs) {
						System.out.println(df.format(new Date(rj.getStatusTimestamp(rj.getState())))
								+" : "+rj.getJobID().toString()+" : "+rj.getJobName());
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
					for(ExecutionGraph rj : scheduledJobs) {
						System.out.println(df.format(new Date(rj.getStatusTimestamp(rj.getState())))
								+" : "+rj.getJobID().toString()+" : "+rj.getJobName());
					}
					System.out.println("--------------------------------------------------------------");
				}
			}
			return 0;
		}
		catch (Throwable t) {
			return handleError(t);
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
		
		try {
			ActorRef jobManager = getJobManager(line);

			if (jobManager == null) {
				printHelpForCancel();
				return 1;
			}

			AkkaUtils.ask(jobManager, new CancelJob(jobId), getAkkaTimeout());
			return 0;
		}
		catch (Throwable t) {
			return handleError(t);
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
			System.out.println("Error: Jar file: '"+jarFile+"' does not exist.");
			return null;
		}
		else if (!jarFile.isFile()) {
			System.out.println("Error: Jar file '"+jarFile+"' is not a file.");
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
	
	protected String getJobManagerAddressString(CommandLine line) throws IOException {
		Configuration configuration = getGlobalConfiguration();
		
		// first, check if the address comes from the command line option
		if (line.hasOption(ADDRESS_OPTION.getOpt())) {
			try {
				return line.getOptionValue(ADDRESS_OPTION.getOpt());
			}
			catch (Exception e) {
				System.out.println("Error: The JobManager address has an invalid format. " + e.getMessage());
				return null;
			}
		}
		else {
			Properties yarnProps = getYarnProperties();
			if(yarnProps != null) {
				try {
					String address = yarnProps.getProperty(YARN_PROPERTIES_JOBMANAGER_KEY);
					System.out.println("Found a yarn properties file (" + YARN_PROPERTIES_FILE + ") file, "
							+ "using \""+address+"\" to connect to the JobManager");
					return address;
				} catch (Exception e) {
					System.out.println("Found a yarn properties " + YARN_PROPERTIES_FILE + " file, but could not read the JobManager address from the file. "
								+ e.getMessage());
					return null;
				}
			} else {
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
				
				return jobManagerAddress + ":" + jobManagerPort;
			}
		}
	}
	
	protected ActorRef getJobManager(CommandLine line) throws IOException {
		//TODO: Get ActorRef from YarnCluster if we are in YARN mode.
		String jobManagerAddressStr = getJobManagerAddressString(line);
		if (jobManagerAddressStr == null) {
			return null;
		}

		return JobManager.getJobManager(RemoteExecutor.getInetFromHostport(jobManagerAddressStr),
				ActorSystem.create("CliFrontendActorSystem",
						AkkaUtils.getDefaultAkkaConfig()),getAkkaTimeout());
	}
	

	public String getConfigurationDirectory() {
		if(configurationDirectory == null) {
			configurationDirectory = getConfigurationDirectoryFromEnv();
		}
		return configurationDirectory;
	}

	/**
	 * Reads configuration settings. The default path can be overridden
	 * by setting the ENV variable "FLINK_CONF_DIR".
	 *
	 * @return Flink's global configuration
	 */
	protected Configuration getGlobalConfiguration() {
		if (!globalConfigurationLoaded) {
			String location = getConfigurationDirectory();
			GlobalConfiguration.loadConfiguration(location);
			// set default parallelization degree
			Properties yarnProps;
			try {
				yarnProps = getYarnProperties();
				if(yarnProps != null) {
					String propDegree = yarnProps.getProperty(YARN_PROPERTIES_DOP);
					int paraDegree = -1;
					if(propDegree != null) { // maybe the property is not set
						paraDegree = Integer.valueOf(propDegree);
					}
					Configuration c = GlobalConfiguration.getConfiguration();
					if(paraDegree != -1) {
						c.setInteger(ConfigConstants.DEFAULT_PARALLELIZATION_DEGREE_KEY, paraDegree);
					}
					// handle the YARN client's dynamic properties
					String dynamicPropertiesEncoded = yarnProps.getProperty(YARN_PROPERTIES_DYNAMIC_PROPERTIES_STRING);
					List<Tuple2<String, String>> dynamicProperties = getDynamicProperties(dynamicPropertiesEncoded);
					for(Tuple2<String, String> dynamicProperty : dynamicProperties) {
						c.setString(dynamicProperty.f0, dynamicProperty.f1);
					}
					GlobalConfiguration.includeConfiguration(c); // update config
				}
			} catch (IOException e) {
				System.err.println("Error while loading YARN properties: "+e.getMessage());
				e.printStackTrace();
			}

			globalConfigurationLoaded = true;
		}
		return GlobalConfiguration.getConfiguration();
	}
	public static String getConfigurationDirectoryFromEnv() {
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

	protected FiniteDuration getAkkaTimeout(){
		Configuration config = getGlobalConfiguration();

		return AkkaUtils.getTimeout(config);
	}
	
	public static List<Tuple2<String, String>> getDynamicProperties(String dynamicPropertiesEncoded) {
		List<Tuple2<String, String>> ret = new ArrayList<Tuple2<String, String>>();
		if(dynamicPropertiesEncoded != null && dynamicPropertiesEncoded.length() > 0) {
			String[] propertyLines = dynamicPropertiesEncoded.split(CliFrontend.YARN_DYNAMIC_PROPERTIES_SEPARATOR);
			for(String propLine : propertyLines) {
				if(propLine == null) {
					continue;
				}
				String[] kv = propLine.split("=");
				if(kv != null && kv[0] != null && kv[1] != null && kv[0].length() > 0) {
					ret.add(new Tuple2<String, String>(kv[0], kv[1]));
				}
			}
		}
		return ret;
	}
	
	protected Properties getYarnProperties() throws IOException {
		if(!yarnPropertiesLoaded) {
			String loc = getConfigurationDirectory();
			File propertiesFile = new File(loc + '/' + YARN_PROPERTIES_FILE);
			if (propertiesFile.exists()) {
				Properties props = new Properties();
				InputStream is = new FileInputStream( propertiesFile );
				props.load(is);
				yarnProperties = props;
				is.close();
			} else {
				yarnProperties = null;
			}
		}
		return yarnProperties;
	}
	
	protected Client getClient(CommandLine line, ClassLoader classLoader, String programName) throws IOException {
		String jmAddrString = getJobManagerAddressString(line);
		InetSocketAddress jobManagerAddress = null;
		if(jmAddrString.equals(YARN_DEPLOY_JOBMANAGER)) {
			System.out.println("YARN cluster mode detected. Switching Log4j output to console");

			this.runInYarnCluster = true;
			// user wants to run Flink in YARN cluster.
			AbstractFlinkYarnClient flinkYarnClient = yarnSessionCLi.createFlinkYarnClient(line);
			if(flinkYarnClient == null) {
				throw new RuntimeException("Unable to create Flink YARN Client. Check previous log messages");
			}
			try {
				yarnCluster = flinkYarnClient.deploy("Flink Application: "+programName);
			} catch(Exception e) {
				throw new RuntimeException("Error deploying the YARN cluster", e);
			}
			jobManagerAddress = yarnCluster.getJobManagerAddress();
			System.out.println("YARN cluster started");
			System.out.println("JobManager web interface address "+yarnCluster.getWebInterfaceURL());
			System.out.println("Waiting until all TaskManagers have connected");
			while(true) {
				FlinkYarnClusterStatus status = yarnCluster.getClusterStatus();
				if(status != null) {
					if (status.getNumberOfTaskManagers() < flinkYarnClient.getTaskManagerCount()) {
						System.out.println("TaskManager status  (" + status.getNumberOfTaskManagers()+"/"+flinkYarnClient.getTaskManagerCount()+")");
					} else {
						System.out.println("Enough TaskManagers are connected");
						break;
					}
				} else {
					System.out.println("No status updates from YARN cluster received so far. Waiting ...");
				}
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					System.err.println("Thread as interrupted"); Thread.currentThread().interrupt();
				}
			}
		} else {
			jobManagerAddress = RemoteExecutor.getInetFromHostport(jmAddrString);
		}
		return new Client(jobManagerAddress, getGlobalConfiguration(), classLoader);
	}

	/**
	 * Prints the help for the client.
	 */
	private void printHelp() {
		System.out.println("./flink <ACTION> [GENERAL_OPTIONS] [ARGUMENTS]");
		
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(80);
		formatter.setLeftPadding(5);
		formatter.setSyntaxPrefix("  general options:");
		formatter.printHelp(" ", GENERAL_OPTIONS);
		
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
		formatter.setSyntaxPrefix("  additional arguments if -m "+YARN_DEPLOY_JOBMANAGER+" is set:");
		Options yarnOpts = new Options();
		yarnSessionCLi.getYARNSessionCLIOptions(yarnOpts);
		formatter.printHelp(" ", yarnOpts);
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
	 * @param t The exception to display.
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
		final String[] params = Arrays.copyOfRange(args, 1, args.length);
		
		// do action
		if (action.equals(ACTION_RUN)) {
			// run() needs to run in a secured environment for the optimizer.
			if(SecurityUtils.isSecurityEnabled()) {
				System.out.println("Secure Hadoop setup detected.");
				try {
					return SecurityUtils.runSecured(new SecurityUtils.FlinkSecuredRunner<Integer>() {
						@Override
						public Integer run() throws Exception {
							return CliFrontend.this.run(params);
						}
					});
				} catch (Exception e) {
					handleError(e);
				}
			}
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
