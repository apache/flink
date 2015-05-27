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
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
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
import akka.pattern.Patterns;
import akka.util.Timeout;

import org.apache.commons.cli.CommandLine;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.client.cli.CancelOptions;
import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.client.cli.CliFrontendParser;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.cli.CommandLineOptions;
import org.apache.flink.client.cli.InfoOptions;
import org.apache.flink.client.cli.ListOptions;
import org.apache.flink.client.cli.ProgramOptions;
import org.apache.flink.client.cli.RunOptions;
import org.apache.flink.client.program.Client;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.yarn.AbstractFlinkYarnClient;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.messages.JobManagerMessages.CancelJob;
import org.apache.flink.runtime.messages.JobManagerMessages.RunningJobsStatus;
import org.apache.flink.runtime.yarn.AbstractFlinkYarnCluster;
import org.apache.flink.runtime.yarn.FlinkYarnClusterStatus;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Some;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

/**
 * Implementation of a simple command line fronted for executing programs.
 */
public class CliFrontend {

	// actions
	private static final String ACTION_RUN = "run";
	private static final String ACTION_INFO = "info";
	private static final String ACTION_LIST = "list";
	private static final String ACTION_CANCEL = "cancel";

	// config dir parameters
	private static final String ENV_CONFIG_DIRECTORY = "FLINK_CONF_DIR";
	private static final String CONFIG_DIRECTORY_FALLBACK_1 = "../conf";
	private static final String CONFIG_DIRECTORY_FALLBACK_2 = "conf";
	
	// YARN-session related constants
	public static final String YARN_PROPERTIES_FILE = ".yarn-properties";
	public static final String YARN_PROPERTIES_JOBMANAGER_KEY = "jobManager";
	public static final String YARN_PROPERTIES_PARALLELISM = "parallelism";
	public static final String YARN_PROPERTIES_DYNAMIC_PROPERTIES_STRING = "dynamicPropertiesString";

	public static final String YARN_DYNAMIC_PROPERTIES_SEPARATOR = "@@"; // this has to be a regex for String.split()

	/**
	 * A special host name used to run a job by deploying Flink into a YARN cluster,
	 * if this string is specified as the JobManager address
 	 */
	public static final String YARN_DEPLOY_JOBMANAGER = "yarn-cluster";
	

	// --------------------------------------------------------------------------------------------
	// --------------------------------------------------------------------------------------------

	private static final Logger LOG = LoggerFactory.getLogger(CliFrontend.class);

	private final File configDirectory;

	private final Configuration config;

	private final FiniteDuration askTimeout;

	private final FiniteDuration lookupTimeout;

	private InetSocketAddress jobManagerAddress;

	private ActorSystem actorSystem;

	private AbstractFlinkYarnCluster yarnCluster;



	/**
	 *
	 * @throws Exception Thrown if teh configuration directory was not found, the configuration could not
	 *                   be loaded, or the YARN properties could not be parsed.
	 */
	public CliFrontend() throws Exception {
		this(getConfigurationDirectoryFromEnv());
	}

	public CliFrontend(String configDir) throws Exception {

		// configure the config directory
		this.configDirectory = new File(configDir);
		LOG.info("Using configuration directory " + this.configDirectory.getAbsolutePath());

		// load the configuration
		LOG.info("Trying to load configuration file");
		GlobalConfiguration.loadConfiguration(this.configDirectory.getAbsolutePath());
		this.config = GlobalConfiguration.getConfiguration();

		// load the YARN properties
		File propertiesFile = new File(configDirectory, YARN_PROPERTIES_FILE);
		if (propertiesFile.exists()) {

			logAndSysout("Found YARN properties file " + propertiesFile.getAbsolutePath());

			Properties yarnProperties = new Properties();
			try {
				InputStream is = new FileInputStream(propertiesFile);
				try {
					yarnProperties.load(is);
				}
				finally {
					is.close();
				}
			}
			catch (IOException e) {
				throw new Exception("Cannot read the YARN properties file", e);
			}

			// configure the default parallelism from YARN
			String propParallelism = yarnProperties.getProperty(YARN_PROPERTIES_PARALLELISM);
			if (propParallelism != null) { // maybe the property is not set
				try {
					int parallelism = Integer.parseInt(propParallelism);
					this.config.setInteger(ConfigConstants.DEFAULT_PARALLELISM_KEY, parallelism);

					logAndSysout("YARN properties set default parallelism to " + parallelism);
				}
				catch (NumberFormatException e) {
					throw new Exception("Error while parsing the YARN properties: " +
							"Property " + YARN_PROPERTIES_PARALLELISM + " is not an integer.");
				}
			}

			// get the JobManager address from the YARN properties
			String address = yarnProperties.getProperty(YARN_PROPERTIES_JOBMANAGER_KEY);
			if (address != null) {
				try {
					jobManagerAddress = parseJobManagerAddress(address);
				}
				catch (Exception e) {
					throw new Exception("YARN properties contain an invalid entry for JobManager address.", e);
				}

				logAndSysout("Using JobManager address from YARN properties " + jobManagerAddress);
			}

			// handle the YARN client's dynamic properties
			String dynamicPropertiesEncoded = yarnProperties.getProperty(YARN_PROPERTIES_DYNAMIC_PROPERTIES_STRING);
			List<Tuple2<String, String>> dynamicProperties = getDynamicProperties(dynamicPropertiesEncoded);
			for (Tuple2<String, String> dynamicProperty : dynamicProperties) {
				this.config.setString(dynamicProperty.f0, dynamicProperty.f1);
			}
		}

		this.askTimeout = AkkaUtils.getTimeout(config);
		this.lookupTimeout = AkkaUtils.getLookupTimeout(config);
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
		LOG.info("Running 'run' command.");

		RunOptions options;
		try {
			options = CliFrontendParser.parseRunCommand(args);
		}
		catch (CliArgsException e) {
			return handleArgException(e);
		}
		catch (Throwable t) {
			return handleError(t);
		}

		// evaluate help flag
		if (options.isPrintHelp()) {
			CliFrontendParser.printHelpForRun();
			return 0;
		}

		if (options.getJarFilePath() == null) {
			return handleArgException(new CliArgsException("The program JAR file was not specified."));
		}

		PackagedProgram program;
		try {
			LOG.info("Building program from JAR file");
			program = buildProgram(options);
		}
		catch (FileNotFoundException e) {
			return handleArgException(e);
		}
		catch (ProgramInvocationException e) {
			return handleError(e);
		}
		catch (Throwable t) {
			return handleError(t);
		}

		try {
			int userParallelism = options.getParallelism();
			LOG.debug("User parallelism is set to {}", userParallelism);

			Client client = getClient(options, program.getUserCodeClassLoader(), program.getMainClassName(), userParallelism);
			LOG.debug("Client slots is set to {}", client.getMaxSlots());
			if(client.getMaxSlots() != -1 && userParallelism == -1) {
				logAndSysout("Using the parallelism provided by the remote cluster ("+client.getMaxSlots()+"). " +
						"To use another parallelism, set it at the ./bin/flink client.");
				userParallelism = client.getMaxSlots();
			}
			int exitCode = 0;

			// check if detached per job yarn cluster is used to start flink
			if(yarnCluster != null && yarnCluster.isDetached()) {
				logAndSysout("The Flink YARN client has been started in detached mode. In order to stop " +
						"Flink on YARN, use the following command or a YARN web interface to stop it:\n" +
						"yarn application -kill "+yarnCluster.getApplicationId()+"\n" +
						"Please also note that the temporary files of the YARN session in the home directoy will not be removed.");
				executeProgram(program, client, userParallelism, false);
			} else {
				// regular (blocking) execution.
				exitCode = executeProgram(program, client, userParallelism, true);
			}

			// show YARN cluster status if its not a detached YARN cluster.
			if (yarnCluster != null && !yarnCluster.isDetached()) {
				List<String> msgs = yarnCluster.getNewMessages();
				if (msgs != null && msgs.size() > 1) {

					logAndSysout("The following messages were created by the YARN cluster while running the Job:");
					for (String msg : msgs) {
						logAndSysout(msg);
					}
				}
				if (yarnCluster.hasFailed()) {
					logAndSysout("YARN cluster is in failed state!");
					logAndSysout("YARN Diagnostics: " + yarnCluster.getDiagnostics());
				}
			}

			return exitCode;
		}
		catch (Throwable t) {
			return handleError(t);
		}
		finally {
			if (yarnCluster != null && !yarnCluster.isDetached()) {
				logAndSysout("Shutting down YARN cluster");
				yarnCluster.shutdown();
			}
			if (program != null) {
				program.deleteExtractedLibraries();
			}
		}
	}

	/**
	 * Executes the info action.
	 * 
	 * @param args Command line arguments for the info action. 
	 */
	protected int info(String[] args) {
		LOG.info("Running 'info' command.");

		// Parse command line options
		InfoOptions options;
		try {
			options = CliFrontendParser.parseInfoCommand(args);
		}
		catch (CliArgsException e) {
			return handleArgException(e);
		}
		catch (Throwable t) {
			return handleError(t);
		}

		// evaluate help flag
		if (options.isPrintHelp()) {
			CliFrontendParser.printHelpForInfo();
			return 0;
		}

		if (options.getJarFilePath() == null) {
			return handleArgException(new CliArgsException("The program JAR file was not specified."));
		}

		// -------- build the packaged program -------------
		
		PackagedProgram program;
		try {
			LOG.info("Building program from JAR file");
			program = buildProgram(options);
		}
		catch (Throwable t) {
			return handleError(t);
		}
		
		try {
			int parallelism = options.getParallelism();

			LOG.info("Creating program plan dump");
			Client client = getClient(options, program.getUserCodeClassLoader(), program.getMainClassName(), parallelism);
			String jsonPlan = client.getOptimizedPlanAsJson(program, parallelism);

			if (jsonPlan != null) {
				System.out.println("----------------------- Execution Plan -----------------------");
				System.out.println(jsonPlan);
				System.out.println("--------------------------------------------------------------");
			}
			else {
				System.out.println("JSON plan could not be generated.");
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
		LOG.info("Running 'list' command.");

		ListOptions options;
		try {
			options = CliFrontendParser.parseListCommand(args);
		}
		catch (CliArgsException e) {
			return handleArgException(e);
		}
		catch (Throwable t) {
			return handleError(t);
		}

		// evaluate help flag
		if (options.isPrintHelp()) {
			CliFrontendParser.printHelpForList();
			return 0;
		}

		boolean running = options.getRunning();
		boolean scheduled = options.getScheduled();
		
		// print running and scheduled jobs if not option supplied
		if (!running && !scheduled) {
			running = true;
			scheduled = true;
		}
		
		try {
			ActorRef jobManager = getJobManager(options);

			LOG.info("Connecting to JobManager to retrieve list of jobs");
			Future<Object> response = Patterns.ask(jobManager,
					JobManagerMessages.getRequestRunningJobsStatus(), new Timeout(askTimeout));

			Object result;
			try {
				result = Await.result(response, askTimeout);
			}
			catch (Exception e) {
				throw new Exception("Could not retrieve running jobs from the JobManager.", e);
			}

			if (result instanceof RunningJobsStatus) {
				LOG.info("Successfully retrieved list of jobs");

				List<JobStatusMessage> jobs = ((RunningJobsStatus) result).getStatusMessages();

				ArrayList<JobStatusMessage> runningJobs = null;
				ArrayList<JobStatusMessage> scheduledJobs = null;
				if (running) {
					runningJobs = new ArrayList<JobStatusMessage>();
				}
				if (scheduled) {
					scheduledJobs = new ArrayList<JobStatusMessage>();
				}

				for (JobStatusMessage rj : jobs) {
					if (running && rj.getJobState().equals(JobStatus.RUNNING)) {
						runningJobs.add(rj);
					}
					if (scheduled && rj.getJobState().equals(JobStatus.CREATED)) {
						scheduledJobs.add(rj);
					}
				}

				SimpleDateFormat df = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
				Comparator<JobStatusMessage> njec = new Comparator<JobStatusMessage>(){
					@Override
					public int compare(JobStatusMessage o1, JobStatusMessage o2) {
						return (int)(o1.getStartTime()-o2.getStartTime());
					}
				};

				if (running) {
					if(runningJobs.size() == 0) {
						System.out.println("No running jobs.");
					}
					else {
						Collections.sort(runningJobs, njec);

						System.out.println("------------------------ Running Jobs ------------------------");
						for (JobStatusMessage rj : runningJobs) {
							System.out.println(df.format(new Date(rj.getStartTime()))
									+ " : " + rj.getJobId() + " : " + rj.getJobName());
						}
						System.out.println("--------------------------------------------------------------");
					}
				}
				if (scheduled) {
					if (scheduledJobs.size() == 0) {
						System.out.println("No scheduled jobs.");
					}
					else {
						Collections.sort(scheduledJobs, njec);

						System.out.println("----------------------- Scheduled Jobs -----------------------");
						for(JobStatusMessage rj : scheduledJobs) {
							System.out.println(df.format(new Date(rj.getStartTime()))
									+ " : " + rj.getJobId() + " : " + rj.getJobName());
						}
						System.out.println("--------------------------------------------------------------");
					}
				}
				return 0;
			}
			else {
				throw new Exception("ReqeustRunningJobs requires a response of type " +
						"RunningJobs. Instead the response is of type " + result.getClass() + ".");
			}
		}
		catch (Throwable t) {
			return handleError(t);
		}
	}
	
	/**
	 * Executes the CANCEL action.
	 * 
	 * @param args Command line arguments for the cancel action.
	 */
	protected int cancel(String[] args) {
		LOG.info("Running 'cancel' command.");

		CancelOptions options;
		try {
			options = CliFrontendParser.parseCancelCommand(args);
		}
		catch (CliArgsException e) {
			return handleArgException(e);
		}
		catch (Throwable t) {
			return handleError(t);
		}

		// evaluate help flag
		if (options.isPrintHelp()) {
			CliFrontendParser.printHelpForCancel();
			return 0;
		}
		
		String[] cleanedArgs = options.getArgs();
		JobID jobId;

		if (cleanedArgs.length > 0) {
			String jobIdString = cleanedArgs[0];
			try {
				jobId = new JobID(StringUtils.hexStringToByte(jobIdString));
			}
			catch (Exception e) {
				LOG.error("Error: The value for the Job ID is not a valid ID.");
				System.out.println("Error: The value for the Job ID is not a valid ID.");
				return 1;
			}
		}
		else {
			LOG.error("Missing JobID in the command line arguments.");
			System.out.println("Error: Specify a Job ID to cancel a job.");
			return 1;
		}
		
		try {
			ActorRef jobManager = getJobManager(options);
			Future<Object> response = Patterns.ask(jobManager, new CancelJob(jobId), new Timeout(askTimeout));

			try {
				Await.result(response, askTimeout);
				return 0;
			}
			catch (Exception e) {
				throw new Exception("Canceling the job with ID " + jobId + " failed.", e);
			}
		}
		catch (Throwable t) {
			return handleError(t);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Interaction with programs and JobManager
	// --------------------------------------------------------------------------------------------

	protected int executeProgram(PackagedProgram program, Client client, int parallelism, boolean wait) {
		LOG.info("Starting execution of program");
		JobSubmissionResult execResult;
		try {
			client.setPrintStatusDuringExecution(true);
			execResult = client.run(program, parallelism, wait);
		}
		catch (ProgramInvocationException e) {
			return handleError(e);
		}
		finally {
			program.deleteExtractedLibraries();
		}

		if(wait) {
			LOG.info("Program execution finished");
		}

		// we come here after the job has finished (or the job has been submitted)
		if (execResult != null) {
			// if the job has been submitted to a detached YARN cluster, there won't be any
			// exec results, but the object will be set (for the job id)
			if (yarnCluster != null && yarnCluster.isDetached()) {
				if(execResult.getJobID() == null) {
					throw new RuntimeException("Error while starting job. No Job ID set.");
				}
				yarnCluster.stopAfterJob(execResult.getJobID());
				yarnCluster.disconnect();
				System.out.println("The Job has been submitted with JobID "+execResult.getJobID());
				return 0;
			}
			if (execResult instanceof JobExecutionResult) {
				JobExecutionResult result = (JobExecutionResult) execResult;
				System.out.println("Job Runtime: " + result.getNetRuntime() + " ms");
				Map<String, Object> accumulatorsResult = result.getAllAccumulatorResults();
				if (accumulatorsResult.size() > 0) {
					System.out.println("Accumulator Results: ");
					System.out.println(AccumulatorHelper.getResultsFormated(accumulatorsResult));
				}
			} else {
				LOG.info("The Job did not return an execution result");
			}
		}
		return 0;
	}

	/**
	 * Creates a Packaged program from the given command line options.
	 *
	 * @return A PackagedProgram (upon success)
	 * @throws java.io.FileNotFoundException, org.apache.flink.client.program.ProgramInvocationException, java.lang.Throwable
	 */
	protected PackagedProgram buildProgram(ProgramOptions options)
			throws FileNotFoundException, ProgramInvocationException
	{
		String[] programArgs = options.getProgramArgs();
		String jarFilePath = options.getJarFilePath();

		if (jarFilePath == null) {
			throw new IllegalArgumentException("The program JAR file was not specified.");
		}

		File jarFile = new File(jarFilePath);
		
		// Check if JAR file exists
		if (!jarFile.exists()) {
			throw new FileNotFoundException("JAR file does not exist: " + jarFile);
		}
		else if (!jarFile.isFile()) {
			throw new FileNotFoundException("JAR file is not a file: " + jarFile);
		}
		
		// Get assembler class
		String entryPointClass = options.getEntryPointClassName();

		return entryPointClass == null ?
				new PackagedProgram(jarFile, programArgs) :
				new PackagedProgram(jarFile, entryPointClass, programArgs);
	}


	protected InetSocketAddress getJobManagerAddress(CommandLineOptions options) throws Exception {

		// first, check if the address is specified as an option
		if (options.getJobManagerAddress() != null) {
			return parseJobManagerAddress(options.getJobManagerAddress());
		}

		// second, check whether the address was already parsed, or configured through the YARN properties
		if (jobManagerAddress == null) {
			// config file must have the address
			String jobManagerHost = config.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);

			// verify that there is a jobmanager address and port in the configuration
			if (jobManagerHost == null) {
				throw new Exception("Found no configuration in the config directory '" + configDirectory
						+ "' that specifies the JobManager address.");
			}

			int jobManagerPort;
			try {
				jobManagerPort = config.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, -1);
			}
			catch (NumberFormatException e) {
				throw new Exception("Invalid value for the JobManager port (" +
						ConfigConstants.JOB_MANAGER_IPC_PORT_KEY + ") in the configuration.");
			}

			if (jobManagerPort == -1) {
				throw new Exception("Found no configuration in the config directory '" + configDirectory
						+ "' that specifies the JobManager port.");
			}

			jobManagerAddress = new InetSocketAddress(jobManagerHost, jobManagerPort);
		}

		return jobManagerAddress;
	}
	
	protected ActorRef getJobManager(CommandLineOptions options) throws Exception {
		//TODO: Get ActorRef from YarnCluster if we are in YARN mode.

		InetSocketAddress address = getJobManagerAddress(options);

		// start an actor system if needed
		if (this.actorSystem == null) {
			LOG.info("Starting actor system to communicate with JobManager");
			try {
				scala.Tuple2<String, Object> systemEndpoint = new scala.Tuple2<String, Object>("", 0);
				this.actorSystem = AkkaUtils.createActorSystem(config,
						new Some<scala.Tuple2<String, Object>>(systemEndpoint));
			}
			catch (Exception e) {
				throw new IOException("Could not start actor system to communicate with JobManager", e);
			}

			LOG.info("Actor system successfully started");
		}

		LOG.info("Trying to lookup JobManager");
		ActorRef jmActor = JobManager.getJobManagerRemoteReference(address, actorSystem, lookupTimeout);
		LOG.info("JobManager is at " + jmActor.path());
		return jmActor;
	}


	/**
	 * @param userParallelism The parallelism requested by the user in the CLI frontend.
	 */
	protected Client getClient(CommandLineOptions options, ClassLoader classLoader, String programName, int userParallelism) throws Exception {
		InetSocketAddress jobManagerAddress;
		int maxSlots = -1;
		if (YARN_DEPLOY_JOBMANAGER.equals(options.getJobManagerAddress())) {
			logAndSysout("YARN cluster mode detected. Switching Log4j output to console");

			// user wants to run Flink in YARN cluster.
			CommandLine commandLine = options.getCommandLine();
			AbstractFlinkYarnClient flinkYarnClient = CliFrontendParser.getFlinkYarnSessionCli().createFlinkYarnClient(commandLine);

			if (flinkYarnClient == null) {
				throw new RuntimeException("Unable to create Flink YARN Client. Check previous log messages");
			}

			// the number of slots available from YARN:
			int yarnTmSlots = flinkYarnClient.getTaskManagerSlots();
			if(yarnTmSlots == -1) {
				yarnTmSlots = 1;
			}
			maxSlots = yarnTmSlots * flinkYarnClient.getTaskManagerCount();
			if(userParallelism != -1) {
				int slotsPerTM = userParallelism / flinkYarnClient.getTaskManagerCount();
				logAndSysout("The YARN cluster has "+maxSlots+" slots available, but the user requested a parallelism of "+userParallelism+" on YARN. " +
						"Each of the "+flinkYarnClient.getTaskManagerCount()+" TaskManagers will get "+slotsPerTM+" slots.");
				flinkYarnClient.setTaskManagerSlots(slotsPerTM);
			}

			try {
				yarnCluster = flinkYarnClient.deploy("Flink Application: " + programName);
				yarnCluster.connectToCluster();
			}
			catch(Exception e) {
				throw new RuntimeException("Error deploying the YARN cluster", e);
			}

			jobManagerAddress = yarnCluster.getJobManagerAddress();

			logAndSysout("YARN cluster started");
			logAndSysout("JobManager web interface address " + yarnCluster.getWebInterfaceURL());
			logAndSysout("Waiting until all TaskManagers have connected");

			while(true) {
				FlinkYarnClusterStatus status = yarnCluster.getClusterStatus();
				if (status != null) {
					if (status.getNumberOfTaskManagers() < flinkYarnClient.getTaskManagerCount()) {
						logAndSysout("TaskManager status (" + status.getNumberOfTaskManagers() + "/" + flinkYarnClient.getTaskManagerCount() + ")");
					} else {
						logAndSysout("All TaskManagers are connected");
						break;
					}
				} else {
					logAndSysout("No status updates from the YARN cluster received so far. Waiting ...");
				}

				try {
					Thread.sleep(500);
				}
				catch (InterruptedException e) {
					LOG.error("Interrupted while waiting for TaskManagers");
					System.err.println("Thread is interrupted");
					Thread.currentThread().interrupt();
				}
			}
		}
		else {
			jobManagerAddress = getJobManagerAddress(options);
		}
		return new Client(jobManagerAddress, config, classLoader, maxSlots);
	}

	// --------------------------------------------------------------------------------------------
	//  Logging and Exception Handling
	// --------------------------------------------------------------------------------------------

	/**
	 * Displays an exception message for incorrect command line arguments.
	 *
	 * @param e The exception to display.
	 * @return The return code for the process.
	 */
	private int handleArgException(Exception e) {
		LOG.error("Invalid command line arguments." + (e.getMessage() == null ? "" : e.getMessage()));

		System.out.println(e.getMessage());
		System.out.println();
		System.out.println("Use the help option (-h or --help) to get help on the command.");
		return 1;
	}
	/**
	 * Displays an exception message.
	 * 
	 * @param t The exception to display.
	 * @return The return code for the process.
	 */
	private int handleError(Throwable t) {
		LOG.error("Error while running the command.", t);

		t.printStackTrace();
		System.err.println();
		System.err.println("The exception above occurred while trying to run your command.");
		return 1;
	}

	private void logAndSysout(String message) {
		LOG.info(message);
		System.out.println(message);
	}

	// --------------------------------------------------------------------------------------------
	//  Entry point for executable
	// --------------------------------------------------------------------------------------------

	/**
	 * Parses the command line arguments and starts the requested action.
	 * 
	 * @param args command line arguments of the client.
	 * @return The return code of the program
	 */
	public int parseParameters(String[] args) {
		
		// check for action
		if (args.length < 1) {
			CliFrontendParser.printHelp();
			System.out.println("Please specify an action.");
			return 1;
		}
		
		// get action
		String action = args[0];

		// remove action from parameters
		final String[] params = Arrays.copyOfRange(args, 1, args.length);
		
		// do action
		if (action.equals(ACTION_RUN)) {
			// run() needs to run in a secured environment for the optimizer.
			if (SecurityUtils.isSecurityEnabled()) {
				String message = "Secure Hadoop environment setup detected. Running in secure context.";
				LOG.info(message);
				System.out.println(message);

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
		}
		else if (action.equals(ACTION_LIST)) {
			return list(params);
		}
		else if (action.equals(ACTION_INFO)) {
			return info(params);
		}
		else if (action.equals(ACTION_CANCEL)) {
			return cancel(params);
		}
		else if (action.equals("-h") || action.equals("--help")) {
			CliFrontendParser.printHelp();
			return 0;
		}
		else {
			System.out.printf("\"%s\" is not a valid action.\n", action);
			System.out.println();
			System.out.println("Valid actions are \"run\", \"list\", \"info\", or \"cancel\".");
			System.out.println();
			System.out.println("Specify the help option (-h or --help) to get help on the command.");
			return 1;
		}
	}

	public void shutdown() {
		ActorSystem sys = this.actorSystem;
		if (sys != null) {
			this.actorSystem = null;
			sys.shutdown();
		}
	}

	/**
	 * Submits the job based on the arguments
	 */
	public static void main(String[] args) {
		EnvironmentInformation.logEnvironmentInfo(LOG, "Command Line Client", args);
		EnvironmentInformation.checkJavaVersion();

		try {
			CliFrontend cli = new CliFrontend();
			int retCode = cli.parseParameters(args);
			System.exit(retCode);
		}
		catch (Throwable t) {
			LOG.error("Fatal error while running command line interface.", t);
			t.printStackTrace();
			System.exit(31);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Miscellaneous Utilities
	// --------------------------------------------------------------------------------------------

	private static InetSocketAddress parseJobManagerAddress(String hostAndPort) {
		URI uri;
		try {
			uri = new URI("my://" + hostAndPort);
		} catch (URISyntaxException e) {
			throw new RuntimeException("Malformed address " + hostAndPort, e);
		}
		String host = uri.getHost();
		int port = uri.getPort();
		if (host == null || port == -1) {
			throw new RuntimeException("Address is missing hostname or port " + hostAndPort);
		}
		return new InetSocketAddress(host, port);
	}

	public static String getConfigurationDirectoryFromEnv() {
		String location = System.getenv(ENV_CONFIG_DIRECTORY);

		if (location != null) {
			if (new File(location).exists()) {
				return location;
			}
			else {
				throw new RuntimeException("The config directory '" + location + "', specified in the '" +
						ENV_CONFIG_DIRECTORY + "' environment variable, does not exist.");
			}
		}
		else if (new File(CONFIG_DIRECTORY_FALLBACK_1).exists()) {
			location = CONFIG_DIRECTORY_FALLBACK_1;
		}
		else if (new File(CONFIG_DIRECTORY_FALLBACK_2).exists()) {
			location = CONFIG_DIRECTORY_FALLBACK_2;
		}
		else {
			throw new RuntimeException("The configuration directory was not specified. " +
					"Please specify the directory containing the configuration file through the '" +
					ENV_CONFIG_DIRECTORY + "' environment variable.");
		}
		return location;
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
				if (kv.length >= 2 && kv[0] != null && kv[1] != null && kv[0].length() > 0) {
					ret.add(new Tuple2<String, String>(kv[0], kv[1]));
				}
			}
		}
		return ret;
	}
}
