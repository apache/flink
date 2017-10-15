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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.client.cli.CancelOptions;
import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.client.cli.CommandLineOptions;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.client.cli.Flip6DefaultCLI;
import org.apache.flink.client.cli.InfoOptions;
import org.apache.flink.client.cli.ListOptions;
import org.apache.flink.client.cli.ProgramOptions;
import org.apache.flink.client.cli.RunOptions;
import org.apache.flink.client.cli.SavepointOptions;
import org.apache.flink.client.cli.StopOptions;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.program.ProgramMissingJobException;
import org.apache.flink.client.program.ProgramParametrizationException;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.optimizer.plan.FlinkPlan;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.StreamingPlan;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import static org.apache.flink.runtime.messages.JobManagerMessages.DisposeSavepoint;
import static org.apache.flink.runtime.messages.JobManagerMessages.DisposeSavepointFailure;

/**
 * Implementation of a simple command line frontend for executing programs.
 */
public class CliFrontend {

	private static final Logger LOG = LoggerFactory.getLogger(CliFrontend.class);

	// actions
	private static final String ACTION_RUN = "run";
	private static final String ACTION_INFO = "info";
	private static final String ACTION_LIST = "list";
	private static final String ACTION_CANCEL = "cancel";
	private static final String ACTION_STOP = "stop";
	private static final String ACTION_SAVEPOINT = "savepoint";

	// config dir parameters
	private static final String CONFIG_DIRECTORY_FALLBACK_1 = "../conf";
	private static final String CONFIG_DIRECTORY_FALLBACK_2 = "conf";

	// --------------------------------------------------------------------------------------------

	private static final List<CustomCommandLine<?>> customCommandLines = new ArrayList<>(3);

	static {
		//	Command line interface of the YARN session, with a special initialization here
		//	to prefix all options with y/yarn.
		//	Tips: DefaultCLI must be added at last, because getActiveCustomCommandLine(..) will get the
		//	      active CustomCommandLine in order and DefaultCLI isActive always return true.
		final String flinkYarnSessionCLI = "org.apache.flink.yarn.cli.FlinkYarnSessionCli";
		final String flinkYarnCLI = "org.apache.flink.yarn.cli.FlinkYarnCLI";
		try {
			customCommandLines.add(loadCustomCommandLine(flinkYarnSessionCLI, "y", "yarn"));
		} catch (Exception e) {
			LOG.warn("Could not load CLI class {}.", flinkYarnSessionCLI, e);
		}

		try {
			customCommandLines.add(loadCustomCommandLine(flinkYarnCLI, "y", "yarn"));
		} catch (Exception e) {
			LOG.warn("Could not load CLI class {}.", flinkYarnCLI, e);
		}

		customCommandLines.add(new Flip6DefaultCLI());
		customCommandLines.add(new DefaultCLI());
	}

	// --------------------------------------------------------------------------------------------

	private final Configuration config;

	private final String configurationDirectory;

	private final FiniteDuration clientTimeout;

	private final int defaultParallelism;

	/**
	 *
	 * @throws Exception Thrown if the configuration directory was not found, the configuration could not be loaded
	 */
	public CliFrontend() throws Exception {
		this(getConfigurationDirectoryFromEnv());
	}

	public CliFrontend(String configDir) throws Exception {
		configurationDirectory = Preconditions.checkNotNull(configDir);

		// configure the config directory
		File configDirectory = new File(configDir);
		LOG.info("Using configuration directory " + configDirectory.getAbsolutePath());

		// load the configuration
		LOG.info("Trying to load configuration file");
		this.config = GlobalConfiguration.loadConfiguration(configDirectory.getAbsolutePath());

		try {
			FileSystem.initialize(config);
		} catch (IOException e) {
			throw new Exception("Error while setting the default " +
				"filesystem scheme from configuration.", e);
		}

		this.clientTimeout = AkkaUtils.getClientTimeout(config);
		this.defaultParallelism = GlobalConfiguration.loadConfiguration().getInteger(
														ConfigConstants.DEFAULT_PARALLELISM_KEY,
														ConfigConstants.DEFAULT_PARALLELISM);
	}

	// --------------------------------------------------------------------------------------------
	//  Getter & Setter
	// --------------------------------------------------------------------------------------------

	/**
	 * Getter which returns a copy of the associated configuration.
	 *
	 * @return Copy of the associated configuration
	 */
	public Configuration getConfiguration() {
		Configuration copiedConfiguration = new Configuration();

		copiedConfiguration.addAll(config);

		return copiedConfiguration;
	}

	/**
	 * Returns the configuration directory for the CLI frontend.
	 *
	 * @return Configuration directory
	 */
	public String getConfigurationDirectory() {
		return configurationDirectory;
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
		catch (Throwable t) {
			return handleError(t);
		}

		ClusterClient client = null;
		try {

			client = createClient(options, program);
			client.setPrintStatusDuringExecution(options.getStdoutLogging());
			client.setDetached(options.getDetachedMode());
			LOG.debug("Client slots is set to {}", client.getMaxSlots());

			LOG.debug(options.getSavepointRestoreSettings().toString());

			int userParallelism = options.getParallelism();
			LOG.debug("User parallelism is set to {}", userParallelism);
			if (client.getMaxSlots() != -1 && userParallelism == -1) {
				logAndSysout("Using the parallelism provided by the remote cluster ("
					+ client.getMaxSlots() + "). "
					+ "To use another parallelism, set it at the ./bin/flink client.");
				userParallelism = client.getMaxSlots();
			} else if (ExecutionConfig.PARALLELISM_DEFAULT == userParallelism) {
				userParallelism = defaultParallelism;
			}

			return executeProgram(program, client, userParallelism);
		}
		catch (Throwable t) {
			return handleError(t);
		}
		finally {
			if (client != null) {
				try {
					client.shutdown();
				} catch (Exception e) {
					LOG.warn("Could not properly shut down the cluster client.", e);
				}
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
			if (ExecutionConfig.PARALLELISM_DEFAULT == parallelism) {
				parallelism = defaultParallelism;
			}

			LOG.info("Creating program plan dump");

			Optimizer compiler = new Optimizer(new DataStatistics(), new DefaultCostEstimator(), config);
			FlinkPlan flinkPlan = ClusterClient.getOptimizedPlan(compiler, program, parallelism);

			String jsonPlan = null;
			if (flinkPlan instanceof OptimizedPlan) {
				jsonPlan = new PlanJSONDumpGenerator().getOptimizerPlanAsJSON((OptimizedPlan) flinkPlan);
			} else if (flinkPlan instanceof StreamingPlan) {
				jsonPlan = ((StreamingPlan) flinkPlan).getStreamingPlanAsJSON();
			}

			if (jsonPlan != null) {
				System.out.println("----------------------- Execution Plan -----------------------");
				System.out.println(jsonPlan);
				System.out.println("--------------------------------------------------------------");
			}
			else {
				System.out.println("JSON plan could not be generated.");
			}

			String description = program.getDescription();
			if (description != null) {
				System.out.println();
				System.out.println(description);
			}
			else {
				System.out.println();
				System.out.println("No description provided.");
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
			CustomCommandLine<?> activeCommandLine = getActiveCustomCommandLine(options.getCommandLine());
			ClusterClient client = activeCommandLine.retrieveCluster(options.getCommandLine(), config, configurationDirectory);

			Collection<JobStatusMessage> jobDetails;
			try {
				CompletableFuture<Collection<JobStatusMessage>> jobDetailsFuture = client.listJobs();

				try {
					logAndSysout("Waiting for response...");
					jobDetails = jobDetailsFuture.get();
				}
				catch (ExecutionException ee) {
					Throwable cause = ExceptionUtils.stripExecutionException(ee);
					throw new Exception("Failed to retrieve job list.", cause);
				}
			} finally {
				client.shutdown();
			}

			LOG.info("Successfully retrieved list of jobs");

			SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
			Comparator<JobStatusMessage> startTimeComparator = (o1, o2) -> (int) (o1.getStartTime() - o2.getStartTime());

			final List<JobStatusMessage> runningJobs = new ArrayList<>();
			final List<JobStatusMessage> scheduledJobs = new ArrayList<>();
			jobDetails.forEach(details -> {
				if (details.getJobState() == JobStatus.CREATED) {
					scheduledJobs.add(details);
				} else {
					runningJobs.add(details);
				}
			});

			if (running) {
				if (runningJobs.size() == 0) {
					System.out.println("No running jobs.");
				}
				else {
					runningJobs.sort(startTimeComparator);

					System.out.println("------------------ Running/Restarting Jobs -------------------");
					for (JobStatusMessage runningJob : runningJobs) {
						System.out.println(dateFormat.format(new Date(runningJob.getStartTime()))
							+ " : " + runningJob.getJobId() + " : " + runningJob.getJobName() + " (" + runningJob.getJobState() + ")");
					}
					System.out.println("--------------------------------------------------------------");
				}
			}
			if (scheduled) {
				if (scheduledJobs.size() == 0) {
					System.out.println("No scheduled jobs.");
				}
				else {
					scheduledJobs.sort(startTimeComparator);

					System.out.println("----------------------- Scheduled Jobs -----------------------");
					for (JobStatusMessage scheduledJob : scheduledJobs) {
						System.out.println(dateFormat.format(new Date(scheduledJob.getStartTime()))
							+ " : " + scheduledJob.getJobId() + " : " + scheduledJob.getJobName());
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
	 * Executes the STOP action.
	 *
	 * @param args Command line arguments for the stop action.
	 */
	protected int stop(String[] args) {
		LOG.info("Running 'stop' command.");

		StopOptions options;
		try {
			options = CliFrontendParser.parseStopCommand(args);
		}
		catch (CliArgsException e) {
			return handleArgException(e);
		}
		catch (Throwable t) {
			return handleError(t);
		}

		// evaluate help flag
		if (options.isPrintHelp()) {
			CliFrontendParser.printHelpForStop();
			return 0;
		}

		String[] stopArgs = options.getArgs();
		JobID jobId;

		if (stopArgs.length > 0) {
			String jobIdString = stopArgs[0];
			try {
				jobId = new JobID(StringUtils.hexStringToByte(jobIdString));
			}
			catch (Exception e) {
				return handleError(e);
			}
		}
		else {
			return handleArgException(new CliArgsException("Missing JobID"));
		}

		try {
			CustomCommandLine<?> activeCommandLine = getActiveCustomCommandLine(options.getCommandLine());
			ClusterClient client = activeCommandLine.retrieveCluster(options.getCommandLine(), config, configurationDirectory);
			try {
				logAndSysout("Stopping job " + jobId + '.');
				client.stop(jobId);
				logAndSysout("Stopped job " + jobId + '.');

				return 0;
			} finally {
				client.shutdown();
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

		boolean withSavepoint = options.isWithSavepoint();
		String targetDirectory = options.getSavepointTargetDirectory();

		JobID jobId;

		// Figure out jobID. This is a little overly complicated, because
		// we have to figure out whether the optional target directory
		// is set:
		// - cancel -s <jobID> => default target dir (JobID parsed as opt arg)
		// - cancel -s <targetDir> <jobID> => custom target dir (parsed correctly)
		if (cleanedArgs.length > 0) {
			String jobIdString = cleanedArgs[0];
			try {
				jobId = new JobID(StringUtils.hexStringToByte(jobIdString));
			} catch (Exception e) {
				LOG.error("Error: The value for the Job ID is not a valid ID.");
				System.out.println("Error: The value for the Job ID is not a valid ID.");
				return 1;
			}
		} else if (targetDirectory != null)  {
			// Try this for case: cancel -s <jobID> (default savepoint target dir)
			String jobIdString = targetDirectory;
			try {
				jobId = new JobID(StringUtils.hexStringToByte(jobIdString));
				targetDirectory = null;
			} catch (Exception e) {
				LOG.error("Missing JobID in the command line arguments.");
				System.out.println("Error: Specify a Job ID to cancel a job.");
				return 1;
			}
		} else {
			LOG.error("Missing JobID in the command line arguments.");
			System.out.println("Error: Specify a Job ID to cancel a job.");
			return 1;
		}

		try {
			CustomCommandLine<?> activeCommandLine = getActiveCustomCommandLine(options.getCommandLine());
			ClusterClient client = activeCommandLine.retrieveCluster(options.getCommandLine(), config, configurationDirectory);
			try {
				if (withSavepoint) {
					if (targetDirectory == null) {
						logAndSysout("Cancelling job " + jobId + " with savepoint to default savepoint directory.");
					} else {
						logAndSysout("Cancelling job " + jobId + " with savepoint to " + targetDirectory + '.');
					}
					String savepointPath = client.cancelWithSavepoint(jobId, targetDirectory);
					logAndSysout("Cancelled job " + jobId + ". Savepoint stored in " + savepointPath + '.');
				} else {
					logAndSysout("Cancelling job " + jobId + '.');
					client.cancel(jobId);
					logAndSysout("Cancelled job " + jobId + '.');
				}

				return 0;
			} finally {
				client.shutdown();
			}
		}
		catch (Throwable t) {
			return handleError(t);
		}
	}

	/**
	 * Executes the SAVEPOINT action.
	 *
	 * @param args Command line arguments for the cancel action.
	 */
	protected int savepoint(String[] args) {
		LOG.info("Running 'savepoint' command.");

		SavepointOptions options;
		try {
			options = CliFrontendParser.parseSavepointCommand(args);
		} catch (CliArgsException e) {
			return handleArgException(e);
		} catch (Throwable t) {
			return handleError(t);
		}

		// evaluate help flag
		if (options.isPrintHelp()) {
			CliFrontendParser.printHelpForSavepoint();
			return 0;
		}

		if (options.isDispose()) {
			// Discard
			return disposeSavepoint(options);
		} else {
			// Trigger
			String[] cleanedArgs = options.getArgs();
			JobID jobId;

			if (cleanedArgs.length >= 1) {
				String jobIdString = cleanedArgs[0];
				try {
					jobId = new JobID(StringUtils.hexStringToByte(jobIdString));
				} catch (Exception e) {
					return handleArgException(new IllegalArgumentException(
							"Error: The value for the Job ID is not a valid ID."));
				}
			} else {
				return handleArgException(new IllegalArgumentException(
						"Error: The value for the Job ID is not a valid ID. " +
								"Specify a Job ID to trigger a savepoint."));
			}

			String savepointDirectory = null;
			if (cleanedArgs.length >= 2) {
				savepointDirectory = cleanedArgs[1];
			}

			// Print superfluous arguments
			if (cleanedArgs.length >= 3) {
				logAndSysout("Provided more arguments than required. Ignoring not needed arguments.");
			}

			return triggerSavepoint(options, jobId, savepointDirectory);
		}
	}

	/**
	 * Sends a {@link org.apache.flink.runtime.messages.JobManagerMessages.TriggerSavepoint}
	 * message to the job manager.
	 */
	private int triggerSavepoint(SavepointOptions options, JobID jobId, String savepointDirectory) {
		try {
			CustomCommandLine<?> activeCommandLine = getActiveCustomCommandLine(options.getCommandLine());
			ClusterClient client = activeCommandLine.retrieveCluster(options.getCommandLine(), config, configurationDirectory);
			try {
				logAndSysout("Triggering savepoint for job " + jobId + ".");
				CompletableFuture<String> savepointPathFuture = client.triggerSavepoint(jobId, savepointDirectory);

				String savepointPath;
				try {
					logAndSysout("Waiting for response...");
					savepointPath = savepointPathFuture.get();
				}
				catch (ExecutionException ee) {
					Throwable cause = ExceptionUtils.stripExecutionException(ee);
					throw new FlinkException("Triggering a savepoint for the job " + jobId + " failed.", cause);
				}

				logAndSysout("Savepoint completed. Path: " + savepointPath);
				logAndSysout("You can resume your program from this savepoint with the run command.");

				return 0;
			}
			finally {
				client.shutdown();
			}
		}
		catch (Throwable t) {
			return handleError(t);
		}
	}

	/**
	 * Sends a {@link org.apache.flink.runtime.messages.JobManagerMessages.DisposeSavepoint}
	 * message to the job manager.
	 */
	private int disposeSavepoint(SavepointOptions options) {
		try {
			String savepointPath = options.getSavepointPath();
			if (savepointPath == null) {
				throw new IllegalArgumentException("Missing required argument: savepoint path. " +
						"Usage: bin/flink savepoint -d <savepoint-path>");
			}

			ActorGateway jobManager = getJobManagerGateway(options);

			logAndSysout("Disposing savepoint '" + savepointPath + "'.");

			Object msg = new DisposeSavepoint(savepointPath);
			Future<Object> response = jobManager.ask(msg, clientTimeout);

			Object result;
			try {
				logAndSysout("Waiting for response...");
				result = Await.result(response, clientTimeout);
			} catch (Exception e) {
				throw new Exception("Disposing the savepoint with path" + savepointPath + " failed.", e);
			}

			if (result.getClass() == JobManagerMessages.getDisposeSavepointSuccess().getClass()) {
				logAndSysout("Savepoint '" + savepointPath + "' disposed.");
				return 0;
			} else if (result instanceof DisposeSavepointFailure) {
				DisposeSavepointFailure failure = (DisposeSavepointFailure) result;

				if (failure.cause() instanceof ClassNotFoundException) {
					throw new ClassNotFoundException("Savepoint disposal failed, because of a " +
							"missing class. This is most likely caused by a custom state " +
							"instance, which cannot be disposed without the user code class " +
							"loader. Please provide the program jar with which you have created " +
							"the savepoint via -j <JAR> for disposal.",
							failure.cause().getCause());
				} else {
					throw failure.cause();
				}
			} else {
				throw new IllegalStateException("Unknown JobManager response of type " +
						result.getClass());
			}
		} catch (Throwable t) {
			return handleError(t);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Interaction with programs and JobManager
	// --------------------------------------------------------------------------------------------

	protected int executeProgram(PackagedProgram program, ClusterClient client, int parallelism) {
		logAndSysout("Starting execution of program");

		JobSubmissionResult result;
		try {
			result = client.run(program, parallelism);
		} catch (ProgramParametrizationException e) {
			return handleParametrizationException(e);
		} catch (ProgramMissingJobException e) {
			return handleMissingJobException();
		} catch (ProgramInvocationException e) {
			return handleError(e);
		} finally {
			program.deleteExtractedLibraries();
		}

		if (null == result) {
			logAndSysout("No JobSubmissionResult returned, please make sure you called " +
				"ExecutionEnvironment.execute()");
			return 1;
		}

		if (result.isJobExecutionResult()) {
			logAndSysout("Program execution finished");
			JobExecutionResult execResult = result.getJobExecutionResult();
			System.out.println("Job with JobID " + execResult.getJobID() + " has finished.");
			System.out.println("Job Runtime: " + execResult.getNetRuntime() + " ms");
			Map<String, Object> accumulatorsResult = execResult.getAllAccumulatorResults();
			if (accumulatorsResult.size() > 0) {
				System.out.println("Accumulator Results: ");
				System.out.println(AccumulatorHelper.getResultsFormatted(accumulatorsResult));
			}
		} else {
			logAndSysout("Job has been submitted with JobID " + result.getJobID());
		}

		return 0;
	}

	/**
	 * Creates a Packaged program from the given command line options.
	 *
	 * @return A PackagedProgram (upon success)
	 * @throws java.io.FileNotFoundException
	 * @throws org.apache.flink.client.program.ProgramInvocationException
	 */
	protected PackagedProgram buildProgram(ProgramOptions options)
			throws FileNotFoundException, ProgramInvocationException {
		String[] programArgs = options.getProgramArgs();
		String jarFilePath = options.getJarFilePath();
		List<URL> classpaths = options.getClasspaths();

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

		PackagedProgram program = entryPointClass == null ?
				new PackagedProgram(jarFile, classpaths, programArgs) :
				new PackagedProgram(jarFile, classpaths, entryPointClass, programArgs);

		program.setSavepointRestoreSettings(options.getSavepointRestoreSettings());

		return program;
	}

	/**
	 * Updates the associated configuration with the given command line options.
	 *
	 * @param options Command line options
	 */
	protected ClusterClient retrieveClient(CommandLineOptions options) {
		CustomCommandLine customCLI = getActiveCustomCommandLine(options.getCommandLine());
		try {
			ClusterClient client = customCLI.retrieveCluster(options.getCommandLine(), config, configurationDirectory);
			logAndSysout("Using address " + client.getJobManagerAddress() + " to connect to JobManager.");
			return client;
		} catch (Exception e) {
			LOG.error("Couldn't retrieve {} cluster.", customCLI.getId(), e);
			throw new IllegalConfigurationException("Couldn't retrieve client for cluster", e);
		}
	}

	/**
	 * Retrieves the {@link ActorGateway} for the JobManager. The ClusterClient is retrieved
	 * from the provided {@link CommandLineOptions}.
	 *
	 * @param options CommandLineOptions specifying the JobManager URL
	 * @return Gateway to the JobManager
	 * @throws Exception
	 */
	protected ActorGateway getJobManagerGateway(CommandLineOptions options) throws Exception {
		logAndSysout("Retrieving JobManager.");
		return retrieveClient(options).getJobManagerGateway();
	}

	/**
	 * Creates a {@link ClusterClient} object from the given command line options and other parameters.
	 * @param options Command line options
	 * @param program The program for which to create the client.
	 * @throws Exception
	 */
	protected ClusterClient createClient(
			CommandLineOptions options,
			PackagedProgram program) throws Exception {

		// Get the custom command-line (e.g. Standalone/Yarn/Mesos)
		CustomCommandLine<?> activeCommandLine = getActiveCustomCommandLine(options.getCommandLine());

		ClusterClient client;
		try {
			client = activeCommandLine.retrieveCluster(options.getCommandLine(), config, configurationDirectory);
			logAndSysout("Cluster configuration: " + client.getClusterIdentifier());
		} catch (UnsupportedOperationException e) {
			try {
				String applicationName = "Flink Application: " + program.getMainClassName();
				client = activeCommandLine.createCluster(
					applicationName,
					options.getCommandLine(),
					config,
					configurationDirectory,
					program.getAllLibraries());
				logAndSysout("Cluster started: " + client.getClusterIdentifier());
			} catch (UnsupportedOperationException e2) {
				throw new IllegalConfigurationException(
					"The JobManager address is neither provided at the command-line, " +
						"nor configured in flink-conf.yaml.");
			}
		}

		// Avoid resolving the JobManager Gateway here to prevent blocking until we invoke the user's program.
		final InetSocketAddress jobManagerAddress = client.getJobManagerAddress();
		logAndSysout("Using address " + jobManagerAddress.getHostString() + ":" + jobManagerAddress.getPort() + " to connect to JobManager.");
		try {
			logAndSysout("JobManager web interface address " + client.getWebInterfaceURL());
		} catch (UnsupportedOperationException uoe) {
			logAndSysout("JobManager web interface not active.");
		}
		return client;
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
		LOG.error("Invalid command line arguments. " + (e.getMessage() == null ? "" : e.getMessage()));

		System.out.println(e.getMessage());
		System.out.println();
		System.out.println("Use the help option (-h or --help) to get help on the command.");
		return 1;
	}

	/**
	 * Displays an optional exception message for incorrect program parametrization.
	 *
	 * @param e The exception to display.
	 * @return The return code for the process.
	 */
	private int handleParametrizationException(ProgramParametrizationException e) {
		System.err.println(e.getMessage());
		return 1;
	}

	/**
	 * Displays a message for a program without a job to execute.
	 *
	 * @return The return code for the process.
	 */
	private int handleMissingJobException() {
		System.err.println();
		System.err.println("The program didn't contain a Flink job. " +
			"Perhaps you forgot to call execute() on the execution environment.");
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

		System.err.println();
		System.err.println("------------------------------------------------------------");
		System.err.println(" The program finished with the following exception:");
		System.err.println();

		if (t.getCause() instanceof InvalidProgramException) {
			System.err.println(t.getCause().getMessage());
			StackTraceElement[] trace = t.getCause().getStackTrace();
			for (StackTraceElement ele: trace) {
				System.err.println("\t" + ele.toString());
				if (ele.getMethodName().equals("main")) {
					break;
				}
			}
		} else {
			t.printStackTrace();
		}
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
		switch (action) {
			case ACTION_RUN:
				return run(params);
			case ACTION_LIST:
				return list(params);
			case ACTION_INFO:
				return info(params);
			case ACTION_CANCEL:
				return cancel(params);
			case ACTION_STOP:
				return stop(params);
			case ACTION_SAVEPOINT:
				return savepoint(params);
			case "-h":
			case "--help":
				CliFrontendParser.printHelp();
				return 0;
			case "-v":
			case "--version":
				String version = EnvironmentInformation.getVersion();
				String commitID = EnvironmentInformation.getRevisionInformation().commitId;
				System.out.print("Version: " + version);
				System.out.println(!commitID.equals(EnvironmentInformation.UNKNOWN) ? ", Commit ID: " + commitID : "");
				return 0;
			default:
				System.out.printf("\"%s\" is not a valid action.\n", action);
				System.out.println();
				System.out.println("Valid actions are \"run\", \"list\", \"info\", \"savepoint\", \"stop\", or \"cancel\".");
				System.out.println();
				System.out.println("Specify the version option (-v or --version) to print Flink version.");
				System.out.println();
				System.out.println("Specify the help option (-h or --help) to get help on the command.");
				return 1;
		}
	}

	/**
	 * Submits the job based on the arguments.
	 */
	public static void main(final String[] args) {
		EnvironmentInformation.logEnvironmentInfo(LOG, "Command Line Client", args);

		try {
			final CliFrontend cli = new CliFrontend();
			SecurityUtils.install(new SecurityConfiguration(cli.config));
			int retCode = SecurityUtils.getInstalledContext()
					.runSecured(new Callable<Integer>() {
						@Override
						public Integer call() {
							return cli.parseParameters(args);
						}
					});
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

	public static String getConfigurationDirectoryFromEnv() {
		String location = System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR);

		if (location != null) {
			if (new File(location).exists()) {
				return location;
			}
			else {
				throw new RuntimeException("The config directory '" + location + "', specified in the '" +
					ConfigConstants.ENV_FLINK_CONF_DIR + "' environment variable, does not exist.");
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
				ConfigConstants.ENV_FLINK_CONF_DIR + "' environment variable.");
		}
		return location;
	}

	/**
	 * Writes the given job manager address to the associated configuration object.
	 *
	 * @param address Address to write to the configuration
	 * @param config The config to write to
	 */
	public static void setJobManagerAddressInConfig(Configuration config, InetSocketAddress address) {
		config.setString(JobManagerOptions.ADDRESS, address.getHostString());
		config.setInteger(JobManagerOptions.PORT, address.getPort());
	}

	// --------------------------------------------------------------------------------------------
	//  Custom command-line
	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the custom command-line for the arguments.
	 * @param commandLine The input to the command-line.
	 * @return custom command-line which is active (may only be one at a time)
	 */
	public CustomCommandLine getActiveCustomCommandLine(CommandLine commandLine) {
		for (CustomCommandLine cli : customCommandLines) {
			if (cli.isActive(commandLine, config)) {
				return cli;
			}
		}
		throw new IllegalStateException("No command-line ran.");
	}

	/**
	 * Retrieves the loaded custom command-lines.
	 * @return An unmodifiyable list of loaded custom command-lines.
	 */
	public static List<CustomCommandLine<?>> getCustomCommandLineList() {
		return Collections.unmodifiableList(customCommandLines);
	}

	/**
	 * Loads a class from the classpath that implements the CustomCommandLine interface.
	 * @param className The fully-qualified class name to load.
	 * @param params The constructor parameters
	 */
	private static CustomCommandLine<?> loadCustomCommandLine(String className, Object... params) throws IllegalAccessException, InvocationTargetException, InstantiationException, ClassNotFoundException, NoSuchMethodException {

		Class<? extends CustomCommandLine> customCliClass =
			Class.forName(className).asSubclass(CustomCommandLine.class);

		// construct class types from the parameters
		Class<?>[] types = new Class<?>[params.length];
		for (int i = 0; i < params.length; i++) {
			Preconditions.checkNotNull(params[i], "Parameters for custom command-lines may not be null.");
			types[i] = params[i].getClass();
		}

		Constructor<? extends CustomCommandLine> constructor = customCliClass.getConstructor(types);

		return constructor.newInstance(params);
	}

}
