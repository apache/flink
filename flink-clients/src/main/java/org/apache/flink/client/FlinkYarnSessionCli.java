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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.yarn.AbstractFlinkYarnClient;
import org.apache.flink.runtime.yarn.AbstractFlinkYarnCluster;
import org.apache.flink.runtime.yarn.FlinkYarnClusterStatus;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Class handling the command line interface to the YARN session.
 */
public class FlinkYarnSessionCli {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkYarnSessionCli.class);

	//------------------------------------ Constants   -------------------------

	private static final String CONFIG_FILE_NAME = "flink-conf.yaml";
	public static final String CONFIG_FILE_LOGBACK_NAME = "logback.xml";
	public static final String CONFIG_FILE_LOG4J_NAME = "log4j.properties";

	private static final int CLIENT_POLLING_INTERVALL = 3;


	//------------------------------------ Command Line argument options -------------------------
	// the prefix transformation is used by the CliFrontend static constructor.
	private final Option QUERY;
	// --- or ---
	private final Option QUEUE;
	private final Option SHIP_PATH;
	private final Option FLINK_JAR;
	private final Option JM_MEMORY;
	private final Option TM_MEMORY;
	private final Option CONTAINER;
	private final Option SLOTS;
	private final Option DETACHED;
	private final Option STREAMING;
	private final Option NAME;

	/**
	 * Dynamic properties allow the user to specify additional configuration values with -D, such as
	 *  -Dfs.overwrite-files=true  -Dtaskmanager.network.numberOfBuffers=16368
	 */
	private final Option DYNAMIC_PROPERTIES;

	//------------------------------------ Internal fields -------------------------
	private AbstractFlinkYarnCluster yarnCluster = null;
	private boolean detachedMode = false;

	public FlinkYarnSessionCli(String shortPrefix, String longPrefix) {
		QUERY = new Option(shortPrefix + "q", longPrefix + "query", false, "Display available YARN resources (memory, cores)");
		QUEUE = new Option(shortPrefix + "qu", longPrefix + "queue", true, "Specify YARN queue.");
		SHIP_PATH = new Option(shortPrefix + "t", longPrefix + "ship", true, "Ship files in the specified directory (t for transfer)");
		FLINK_JAR = new Option(shortPrefix + "j", longPrefix + "jar", true, "Path to Flink jar file");
		JM_MEMORY = new Option(shortPrefix + "jm", longPrefix + "jobManagerMemory", true, "Memory for JobManager Container [in MB]");
		TM_MEMORY = new Option(shortPrefix + "tm", longPrefix + "taskManagerMemory", true, "Memory per TaskManager Container [in MB]");
		CONTAINER = new Option(shortPrefix + "n", longPrefix + "container", true, "Number of YARN container to allocate (=Number of Task Managers)");
		SLOTS = new Option(shortPrefix + "s", longPrefix + "slots", true, "Number of slots per TaskManager");
		DYNAMIC_PROPERTIES = new Option(shortPrefix + "D", true, "Dynamic properties");
		DETACHED = new Option(shortPrefix + "d", longPrefix + "detached", false, "Start detached");
		STREAMING = new Option(shortPrefix + "st", longPrefix + "streaming", false, "Start Flink in streaming mode");
		NAME = new Option(shortPrefix + "nm", longPrefix + "name", true, "Set a custom name for the application on YARN");
	}

	public AbstractFlinkYarnClient createFlinkYarnClient(CommandLine cmd) {

		AbstractFlinkYarnClient flinkYarnClient = getFlinkYarnClient();
		if (flinkYarnClient == null) {
			return null;
		}

		if (!cmd.hasOption(CONTAINER.getOpt())) { // number of containers is required option!
			LOG.error("Missing required argument " + CONTAINER.getOpt());
			printUsage();
			return null;
		}
		flinkYarnClient.setTaskManagerCount(Integer.valueOf(cmd.getOptionValue(CONTAINER.getOpt())));

		// Jar Path
		Path localJarPath;
		if (cmd.hasOption(FLINK_JAR.getOpt())) {
			String userPath = cmd.getOptionValue(FLINK_JAR.getOpt());
			if(!userPath.startsWith("file://")) {
				userPath = "file://" + userPath;
			}
			localJarPath = new Path(userPath);
		} else {
			LOG.info("No path for the flink jar passed. Using the location of "+flinkYarnClient.getClass()+" to locate the jar");
			localJarPath = new Path("file://"+flinkYarnClient.getClass().getProtectionDomain().getCodeSource().getLocation().getPath());
		}

		flinkYarnClient.setLocalJarPath(localJarPath);

		// Conf Path
		String confDirPath = CliFrontend.getConfigurationDirectoryFromEnv();
		GlobalConfiguration.loadConfiguration(confDirPath);
		Configuration flinkConfiguration = GlobalConfiguration.getConfiguration();
		flinkYarnClient.setFlinkConfigurationObject(flinkConfiguration);
		flinkYarnClient.setConfigurationDirectory(confDirPath);
		File confFile = new File(confDirPath + File.separator + CONFIG_FILE_NAME);
		if (!confFile.exists()) {
			LOG.error("Unable to locate configuration file in "+confFile);
			return null;
		}
		Path confPath = new Path(confFile.getAbsolutePath());

		flinkYarnClient.setConfigurationFilePath(confPath);

		List<File> shipFiles = new ArrayList<>();
		// path to directory to ship
		if (cmd.hasOption(SHIP_PATH.getOpt())) {
			String shipPath = cmd.getOptionValue(SHIP_PATH.getOpt());
			File shipDir = new File(shipPath);
			if (shipDir.isDirectory()) {
				shipFiles = new ArrayList<>(Arrays.asList(shipDir.listFiles(new FilenameFilter() {
					@Override
					public boolean accept(File dir, String name) {
						return !(name.equals(".") || name.equals(".."));
					}
				})));
			} else {
				LOG.warn("Ship directory is not a directory. Ignoring it.");
			}
		}

		//check if there is a logback or log4j file
		if (confDirPath.length() > 0) {
			File logback = new File(confDirPath + File.pathSeparator + CONFIG_FILE_LOGBACK_NAME);
			if (logback.exists()) {
				shipFiles.add(logback);
				flinkYarnClient.setFlinkLoggingConfigurationPath(new Path(logback.toURI()));
			}
			File log4j = new File(confDirPath + File.pathSeparator + CONFIG_FILE_LOG4J_NAME);
			if (log4j.exists()) {
				shipFiles.add(log4j);
				if (flinkYarnClient.getFlinkLoggingConfigurationPath() != null) {
					// this means there is already a logback configuration file --> fail
					LOG.warn("The configuration directory ('" + confDirPath + "') contains both LOG4J and " +
							"Logback configuration files. Please delete or rename one of them.");
				} // else
				flinkYarnClient.setFlinkLoggingConfigurationPath(new Path(log4j.toURI()));
			}
		}

		flinkYarnClient.setShipFiles(shipFiles);

		// queue
		if (cmd.hasOption(QUEUE.getOpt())) {
			flinkYarnClient.setQueue(cmd.getOptionValue(QUEUE.getOpt()));
		}

		// JobManager Memory
		if (cmd.hasOption(JM_MEMORY.getOpt())) {
			int jmMemory = Integer.valueOf(cmd.getOptionValue(JM_MEMORY.getOpt()));
			flinkYarnClient.setJobManagerMemory(jmMemory);
		}

		// Task Managers memory
		if (cmd.hasOption(TM_MEMORY.getOpt())) {
			int tmMemory = Integer.valueOf(cmd.getOptionValue(TM_MEMORY.getOpt()));
			flinkYarnClient.setTaskManagerMemory(tmMemory);
		}

		if (cmd.hasOption(SLOTS.getOpt())) {
			int slots = Integer.valueOf(cmd.getOptionValue(SLOTS.getOpt()));
			flinkYarnClient.setTaskManagerSlots(slots);
		}

		String[] dynamicProperties = null;
		if (cmd.hasOption(DYNAMIC_PROPERTIES.getOpt())) {
			dynamicProperties = cmd.getOptionValues(DYNAMIC_PROPERTIES.getOpt());
		}
		String dynamicPropertiesEncoded = StringUtils.join(dynamicProperties,
				CliFrontend.YARN_DYNAMIC_PROPERTIES_SEPARATOR);

		flinkYarnClient.setDynamicPropertiesEncoded(dynamicPropertiesEncoded);

		if (cmd.hasOption(DETACHED.getOpt())) {
			this.detachedMode = true;
			flinkYarnClient.setDetachedMode(detachedMode);
		}

		if(cmd.hasOption(NAME.getOpt())) {
			flinkYarnClient.setName(cmd.getOptionValue(NAME.getOpt()));
		}
		return flinkYarnClient;
	}


	private void printUsage() {
		System.out.println("Usage:");
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(200);
		formatter.setLeftPadding(5);
		formatter.setSyntaxPrefix("   Required");
		Options req = new Options();
		req.addOption(CONTAINER);
		formatter.printHelp(" ", req);

		formatter.setSyntaxPrefix("   Optional");
		Options opt = new Options();
		opt.addOption(JM_MEMORY);
		opt.addOption(TM_MEMORY);
		opt.addOption(QUERY);
		opt.addOption(QUEUE);
		opt.addOption(SLOTS);
		opt.addOption(DYNAMIC_PROPERTIES);
		opt.addOption(DETACHED);
		opt.addOption(STREAMING);
		opt.addOption(NAME);
		formatter.printHelp(" ", opt);
	}

	public static AbstractFlinkYarnClient getFlinkYarnClient() {
		AbstractFlinkYarnClient yarnClient;
		try {
			Class<? extends AbstractFlinkYarnClient> yarnClientClass =
					Class.forName("org.apache.flink.yarn.FlinkYarnClient").asSubclass(AbstractFlinkYarnClient.class);
			yarnClient = InstantiationUtil.instantiate(yarnClientClass, AbstractFlinkYarnClient.class);
		}
		catch (ClassNotFoundException e) {
			System.err.println("Unable to locate the Flink YARN Client. " +
					"Please ensure that you are using a Flink build with Hadoop2/YARN support. Message: " +
					e.getMessage());
			e.printStackTrace(System.err);
			return null; // make it obvious
		}
		return yarnClient;
	}

	private static void writeYarnProperties(Properties properties, File propertiesFile) {
		try {
			OutputStream out = new FileOutputStream(propertiesFile);
			properties.store(out, "Generated YARN properties file");
			out.close();
		} catch (IOException e) {
			throw new RuntimeException("Error writing the properties file", e);
		}
		propertiesFile.setReadable(true, false); // readable for all.
	}

	public static void runInteractiveCli(AbstractFlinkYarnCluster yarnCluster) {
		final String HELP = "Available commands:\n" +
				"help - show these commands\n" +
				"stop - stop the YARN session";
		int numTaskmanagers = 0;
		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
			label:
			while (true) {
				// ------------------ check if there are updates by the cluster -----------

				FlinkYarnClusterStatus status = yarnCluster.getClusterStatus();
				if (status != null && numTaskmanagers != status.getNumberOfTaskManagers()) {
					System.err.println("Number of connected TaskManagers changed to " +
							status.getNumberOfTaskManagers() + ". Slots available: " + status.getNumberOfSlots());
					numTaskmanagers = status.getNumberOfTaskManagers();
				}

				List<String> messages = yarnCluster.getNewMessages();
				if (messages != null && messages.size() > 0) {
					System.err.println("New messages from the YARN cluster: ");
					for (String msg : messages) {
						System.err.println(msg);
					}
				}

				if (yarnCluster.hasFailed()) {
					System.err.println("The YARN cluster has failed");
					yarnCluster.shutdown(true);
				}

				// wait until CLIENT_POLLING_INTERVALL is over or the user entered something.
				long startTime = System.currentTimeMillis();
				while ((System.currentTimeMillis() - startTime) < CLIENT_POLLING_INTERVALL * 1000
						&& !in.ready()) {
					Thread.sleep(200);
				}
				//------------- handle interactive command by user. ----------------------

				if (in.ready()) {
					String command = in.readLine();
					switch (command) {
						case "quit":
						case "stop":
							break label;

						case "help":
							System.err.println(HELP);
							break;
						default:
							System.err.println("Unknown command '" + command + "'. Showing help: \n" + HELP);
							break;
					}
				}
				if (yarnCluster.hasBeenStopped()) {
					LOG.info("Stopping interactive command line interface, YARN cluster has been stopped.");
					break;
				}
			}
		} catch(Exception e) {
			LOG.warn("Exception while running the interactive command line interface", e);
		}
	}

	public static void main(String[] args) {
		FlinkYarnSessionCli cli = new FlinkYarnSessionCli("", ""); // no prefix for the YARN session
		System.exit(cli.run(args));
	}

	public void getYARNSessionCLIOptions(Options options) {
		options.addOption(FLINK_JAR);
		options.addOption(JM_MEMORY);
		options.addOption(TM_MEMORY);
		options.addOption(CONTAINER);
		options.addOption(QUEUE);
		options.addOption(QUERY);
		options.addOption(SHIP_PATH);
		options.addOption(SLOTS);
		options.addOption(DYNAMIC_PROPERTIES);
		options.addOption(DETACHED);
		options.addOption(STREAMING);
		options.addOption(NAME);
	}

	public int run(String[] args) {
		//
		//	Command Line Options
		//
		Options options = new Options();
		getYARNSessionCLIOptions(options);

		CommandLineParser parser = new PosixParser();
		CommandLine cmd;
		try {
			cmd = parser.parse(options, args);
		} catch(Exception e) {
			System.out.println(e.getMessage());
			printUsage();
			return 1;
		}

		// Query cluster for metrics
		if (cmd.hasOption(QUERY.getOpt())) {
			AbstractFlinkYarnClient flinkYarnClient = getFlinkYarnClient();
			String description;
			try {
				description = flinkYarnClient.getClusterDescription();
			} catch (Exception e) {
				System.err.println("Error while querying the YARN cluster for available resources: "+e.getMessage());
				e.printStackTrace(System.err);
				return 1;
			}
			System.out.println(description);
			return 0;
		} else {
			AbstractFlinkYarnClient flinkYarnClient = createFlinkYarnClient(cmd);

			if (flinkYarnClient == null) {
				System.err.println("Error while starting the YARN Client. Please check log output!");
				return 1;
			}


			try {
				yarnCluster = flinkYarnClient.deploy();
				// only connect to cluster if its not a detached session.
				if(!flinkYarnClient.isDetached()) {
					yarnCluster.connectToCluster();
				}
			} catch (Exception e) {
				System.err.println("Error while deploying YARN cluster: "+e.getMessage());
				e.printStackTrace(System.err);
				return 1;
			}
			//------------------ Cluster deployed, handle connection details
			String jobManagerAddress = yarnCluster.getJobManagerAddress().getAddress().getHostAddress() + ":" + yarnCluster.getJobManagerAddress().getPort();
			System.out.println("Flink JobManager is now running on " + jobManagerAddress);
			System.out.println("JobManager Web Interface: " + yarnCluster.getWebInterfaceURL());
			// file that we write into the conf/ dir containing the jobManager address and the dop.

			String defaultPropertiesFileLocation = System.getProperty("java.io.tmpdir");
			String currentUser = System.getProperty("user.name");
			String propertiesFileLocation = yarnCluster.getFlinkConfiguration().getString(ConfigConstants.YARN_PROPERTIES_FILE_LOCATION, defaultPropertiesFileLocation);

			File yarnPropertiesFile = new File(propertiesFileLocation + File.separator + CliFrontend.YARN_PROPERTIES_FILE + currentUser);

			Properties yarnProps = new Properties();
			yarnProps.setProperty(CliFrontend.YARN_PROPERTIES_JOBMANAGER_KEY, jobManagerAddress);
			if (flinkYarnClient.getTaskManagerSlots() != -1) {
				String parallelism =
						Integer.toString(flinkYarnClient.getTaskManagerSlots() * flinkYarnClient.getTaskManagerCount());
				yarnProps.setProperty(CliFrontend.YARN_PROPERTIES_PARALLELISM, parallelism);
			}
			// add dynamic properties
			if (flinkYarnClient.getDynamicPropertiesEncoded() != null) {
				yarnProps.setProperty(CliFrontend.YARN_PROPERTIES_DYNAMIC_PROPERTIES_STRING,
						flinkYarnClient.getDynamicPropertiesEncoded());
			}
			writeYarnProperties(yarnProps, yarnPropertiesFile);

			//------------------ Cluster running, let user control it ------------

			if (detachedMode) {
				// print info and quit:
				LOG.info("The Flink YARN client has been started in detached mode. In order to stop " +
						"Flink on YARN, use the following command or a YARN web interface to stop it:\n" +
						"yarn application -kill "+yarnCluster.getApplicationId()+"\n" +
						"Please also note that the temporary files of the YARN session in {} will not be removed.",
						flinkYarnClient.getSessionFilesDir());
			} else {
				runInteractiveCli(yarnCluster);

				if (!yarnCluster.hasBeenStopped()) {
					LOG.info("Command Line Interface requested session shutdown");
					yarnCluster.shutdown(false);
				}

				try {
					yarnPropertiesFile.delete();
				} catch (Exception e) {
					LOG.warn("Exception while deleting the JobManager address file", e);
				}
			}
		}
		return 0;
	}

	/**
	 * Utility method for tests.
	 */
	public void stop() {
		if (yarnCluster != null) {
			LOG.info("Command line interface is shutting down the yarnCluster");
			yarnCluster.shutdown(false);
		}
	}
}
