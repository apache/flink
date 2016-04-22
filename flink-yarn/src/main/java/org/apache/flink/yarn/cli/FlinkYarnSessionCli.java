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
package org.apache.flink.yarn.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.client.CliFrontend;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.StandaloneClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterClient;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.messages.GetClusterStatusResponse;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Class handling the command line interface to the YARN session.
 */
public class FlinkYarnSessionCli implements CustomCommandLine<YarnClusterClient> {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkYarnSessionCli.class);

	//------------------------------------ Constants   -------------------------

	public static final String CONFIG_FILE_LOGBACK_NAME = "logback.xml";
	public static final String CONFIG_FILE_LOG4J_NAME = "log4j.properties";

	private static final int CLIENT_POLLING_INTERVALL = 3;

	/** The id for the CommandLine interface */
	private static final String ID = "yarn-cluster";

	// YARN-session related constants
	private static final String YARN_PROPERTIES_FILE = ".yarn-properties-";
	private static final String YARN_PROPERTIES_JOBMANAGER_KEY = "jobManager";
	private static final String YARN_PROPERTIES_PARALLELISM = "parallelism";
	private static final String YARN_PROPERTIES_DYNAMIC_PROPERTIES_STRING = "dynamicPropertiesString";

	private static final String YARN_DYNAMIC_PROPERTIES_SEPARATOR = "@@"; // this has to be a regex for String.split()

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

	private final boolean acceptInteractiveInput;
	
	//------------------------------------ Internal fields -------------------------
	private YarnClusterClient yarnCluster = null;
	private boolean detachedMode = false;

	public FlinkYarnSessionCli(String shortPrefix, String longPrefix) {
		this(shortPrefix, longPrefix, true);
	}

	public FlinkYarnSessionCli(String shortPrefix, String longPrefix, boolean acceptInteractiveInput) {
		this.acceptInteractiveInput = acceptInteractiveInput;
		
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

	/**
	 * Resumes from a Flink Yarn properties file
	 * @param flinkConfiguration The flink configuration
	 * @return True if the properties were loaded, false otherwise
	 */
	private boolean resumeFromYarnProperties(Configuration flinkConfiguration) {
		// load the YARN properties
		File propertiesFile = new File(getYarnPropertiesLocation(flinkConfiguration));
		if (!propertiesFile.exists()) {
			return false;
		}

		logAndSysout("Found YARN properties file " + propertiesFile.getAbsolutePath());

		Properties yarnProperties = new Properties();
		try {
			try (InputStream is = new FileInputStream(propertiesFile)) {
				yarnProperties.load(is);
			}
		}
		catch (IOException e) {
			throw new RuntimeException("Cannot read the YARN properties file", e);
		}

		// configure the default parallelism from YARN
		String propParallelism = yarnProperties.getProperty(YARN_PROPERTIES_PARALLELISM);
		if (propParallelism != null) { // maybe the property is not set
			try {
				int parallelism = Integer.parseInt(propParallelism);
				flinkConfiguration.setInteger(ConfigConstants.DEFAULT_PARALLELISM_KEY, parallelism);

				logAndSysout("YARN properties set default parallelism to " + parallelism);
			}
			catch (NumberFormatException e) {
				throw new RuntimeException("Error while parsing the YARN properties: " +
					"Property " + YARN_PROPERTIES_PARALLELISM + " is not an integer.");
			}
		}

		// get the JobManager address from the YARN properties
		String address = yarnProperties.getProperty(YARN_PROPERTIES_JOBMANAGER_KEY);
		InetSocketAddress jobManagerAddress;
		if (address != null) {
			try {
				jobManagerAddress = ClientUtils.parseHostPortAddress(address);
				// store address in config from where it is retrieved by the retrieval service
				CliFrontend.writeJobManagerAddressToConfig(flinkConfiguration, jobManagerAddress);
			}
			catch (Exception e) {
				throw new RuntimeException("YARN properties contain an invalid entry for JobManager address.", e);
			}

			logAndSysout("Using JobManager address from YARN properties " + jobManagerAddress);
		}

		// handle the YARN client's dynamic properties
		String dynamicPropertiesEncoded = yarnProperties.getProperty(YARN_PROPERTIES_DYNAMIC_PROPERTIES_STRING);
		Map<String, String> dynamicProperties = getDynamicProperties(dynamicPropertiesEncoded);
		for (Map.Entry<String, String> dynamicProperty : dynamicProperties.entrySet()) {
			flinkConfiguration.setString(dynamicProperty.getKey(), dynamicProperty.getValue());
		}

		return true;
	}

	public YarnClusterDescriptor createDescriptor(String defaultApplicationName, CommandLine cmd) {


		YarnClusterDescriptor yarnClusterDescriptor = new YarnClusterDescriptor();

		if (!cmd.hasOption(CONTAINER.getOpt())) { // number of containers is required option!
			LOG.error("Missing required argument {}", CONTAINER.getOpt());
			printUsage();
			throw new IllegalArgumentException("Missing required argument " + CONTAINER.getOpt());
		}
		yarnClusterDescriptor.setTaskManagerCount(Integer.valueOf(cmd.getOptionValue(CONTAINER.getOpt())));

		// Jar Path
		Path localJarPath;
		if (cmd.hasOption(FLINK_JAR.getOpt())) {
			String userPath = cmd.getOptionValue(FLINK_JAR.getOpt());
			if(!userPath.startsWith("file://")) {
				userPath = "file://" + userPath;
			}
			localJarPath = new Path(userPath);
		} else {
			LOG.info("No path for the flink jar passed. Using the location of "
				+ yarnClusterDescriptor.getClass() + " to locate the jar");
			localJarPath = new Path("file://" +
				yarnClusterDescriptor.getClass().getProtectionDomain().getCodeSource().getLocation().getPath());
		}

		yarnClusterDescriptor.setLocalJarPath(localJarPath);

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

		yarnClusterDescriptor.setShipFiles(shipFiles);

		// queue
		if (cmd.hasOption(QUEUE.getOpt())) {
			yarnClusterDescriptor.setQueue(cmd.getOptionValue(QUEUE.getOpt()));
		}

		// JobManager Memory
		if (cmd.hasOption(JM_MEMORY.getOpt())) {
			int jmMemory = Integer.valueOf(cmd.getOptionValue(JM_MEMORY.getOpt()));
			yarnClusterDescriptor.setJobManagerMemory(jmMemory);
		}

		// Task Managers memory
		if (cmd.hasOption(TM_MEMORY.getOpt())) {
			int tmMemory = Integer.valueOf(cmd.getOptionValue(TM_MEMORY.getOpt()));
			yarnClusterDescriptor.setTaskManagerMemory(tmMemory);
		}

		if (cmd.hasOption(SLOTS.getOpt())) {
			int slots = Integer.valueOf(cmd.getOptionValue(SLOTS.getOpt()));
			yarnClusterDescriptor.setTaskManagerSlots(slots);
		}

		String[] dynamicProperties = null;
		if (cmd.hasOption(DYNAMIC_PROPERTIES.getOpt())) {
			dynamicProperties = cmd.getOptionValues(DYNAMIC_PROPERTIES.getOpt());
		}
		String dynamicPropertiesEncoded = StringUtils.join(dynamicProperties, YARN_DYNAMIC_PROPERTIES_SEPARATOR);

		yarnClusterDescriptor.setDynamicPropertiesEncoded(dynamicPropertiesEncoded);

		if (cmd.hasOption(DETACHED.getOpt()) || cmd.hasOption(CliFrontendParser.DETACHED_OPTION.getOpt())) {
			this.detachedMode = true;
			yarnClusterDescriptor.setDetachedMode(true);
		}

		if(cmd.hasOption(NAME.getOpt())) {
			yarnClusterDescriptor.setName(cmd.getOptionValue(NAME.getOpt()));
		} else {
			// set the default application name, if none is specified
			if(defaultApplicationName != null) {
				yarnClusterDescriptor.setName(defaultApplicationName);
			}
		}

		// ----- Convenience -----

		// the number of slots available from YARN:
		int yarnTmSlots = yarnClusterDescriptor.getTaskManagerSlots();
		if (yarnTmSlots == -1) {
			yarnTmSlots = 1;
		}

		int maxSlots = yarnTmSlots * yarnClusterDescriptor.getTaskManagerCount();
		int userParallelism = Integer.valueOf(cmd.getOptionValue(CliFrontendParser.PARALLELISM_OPTION.getOpt(), "-1"));
		if (userParallelism != -1) {
			int slotsPerTM = userParallelism / yarnClusterDescriptor.getTaskManagerCount();
			String message = "The YARN cluster has " + maxSlots + " slots available, " +
				"but the user requested a parallelism of " + userParallelism + " on YARN. " +
				"Each of the " + yarnClusterDescriptor.getTaskManagerCount() + " TaskManagers " +
				"will get "+slotsPerTM+" slots.";
			logAndSysout(message);
			yarnClusterDescriptor.setTaskManagerSlots(slotsPerTM);
		}

		return yarnClusterDescriptor;
	}

	@Override
	public YarnClusterClient createClient(String applicationName, CommandLine cmdLine) throws Exception {

		YarnClusterDescriptor yarnClusterDescriptor = createDescriptor(applicationName, cmdLine);

		try {
			return yarnClusterDescriptor.deploy();
		} catch (Exception e) {
			throw new RuntimeException("Error deploying the YARN cluster", e);
		}

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

	public static void runInteractiveCli(YarnClusterClient yarnCluster, boolean readConsoleInput) {
		final String HELP = "Available commands:\n" +
				"help - show these commands\n" +
				"stop - stop the YARN session";
		int numTaskmanagers = 0;
		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
			label:
			while (true) {
				// ------------------ check if there are updates by the cluster -----------

				GetClusterStatusResponse status = yarnCluster.getClusterStatus();
				LOG.debug("Received status message: {}", status);

				if (status != null && numTaskmanagers != status.numRegisteredTaskManagers()) {
					System.err.println("Number of connected TaskManagers changed to " +
							status.numRegisteredTaskManagers() + ". " +
						"Slots available: " + status.totalNumberOfSlots());
					numTaskmanagers = status.numRegisteredTaskManagers();
				}

				List<String> messages = yarnCluster.getNewMessages();
				if (messages != null && messages.size() > 0) {
					System.err.println("New messages from the YARN cluster: ");
					for (String msg : messages) {
						System.err.println(msg);
					}
				}

				if (yarnCluster.getApplicationStatus() != ApplicationStatus.SUCCEEDED) {
					System.err.println("The YARN cluster has failed");
					yarnCluster.shutdown();
				}

				// wait until CLIENT_POLLING_INTERVAL is over or the user entered something.
				long startTime = System.currentTimeMillis();
				while ((System.currentTimeMillis() - startTime) < CLIENT_POLLING_INTERVALL * 1000
						&& (!readConsoleInput || !in.ready()))
				{
					Thread.sleep(200);
				}
				//------------- handle interactive command by user. ----------------------
				
				if (readConsoleInput && in.ready()) {
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
				
				if (yarnCluster.hasBeenShutdown()) {
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

	@Override
	public String getIdentifier() {
		return ID;
	}

	public void addOptions(Options options) {
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

	@Override
	public ClusterClient retrieveCluster(Configuration config) throws Exception {

		if(resumeFromYarnProperties(config)) {
			return new StandaloneClusterClient(config);
		}

		return null;
	}

	public int run(String[] args) {
		//
		//	Command Line Options
		//
		Options options = new Options();
		addOptions(options);

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
			YarnClusterDescriptor flinkYarnClient = new YarnClusterDescriptor();
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

			YarnClusterDescriptor flinkYarnClient;
			try {
				flinkYarnClient = createDescriptor(null, cmd);
			} catch (Exception e) {
				System.err.println("Error while starting the YARN Client. Please check log output!");
				return 1;
			}

			try {
				yarnCluster = flinkYarnClient.deploy();
			} catch (Exception e) {
				System.err.println("Error while deploying YARN cluster: "+e.getMessage());
				e.printStackTrace(System.err);
				return 1;
			}
			//------------------ ClusterClient deployed, handle connection details
			String jobManagerAddress = yarnCluster.getJobManagerAddress().getAddress().getHostAddress() + ":" + yarnCluster.getJobManagerAddress().getPort();
			System.out.println("Flink JobManager is now running on " + jobManagerAddress);
			System.out.println("JobManager Web Interface: " + yarnCluster.getWebInterfaceURL());

			// file that we write into the conf/ dir containing the jobManager address and the dop.
			File yarnPropertiesFile = new File(getYarnPropertiesLocation(yarnCluster.getFlinkConfiguration()));

			Properties yarnProps = new Properties();
			yarnProps.setProperty(YARN_PROPERTIES_JOBMANAGER_KEY, jobManagerAddress);
			if (flinkYarnClient.getTaskManagerSlots() != -1) {
				String parallelism =
						Integer.toString(flinkYarnClient.getTaskManagerSlots() * flinkYarnClient.getTaskManagerCount());
				yarnProps.setProperty(YARN_PROPERTIES_PARALLELISM, parallelism);
			}
			// add dynamic properties
			if (flinkYarnClient.getDynamicPropertiesEncoded() != null) {
				yarnProps.setProperty(YARN_PROPERTIES_DYNAMIC_PROPERTIES_STRING,
						flinkYarnClient.getDynamicPropertiesEncoded());
			}
			writeYarnProperties(yarnProps, yarnPropertiesFile);

			//------------------ ClusterClient running, let user control it ------------

			if (detachedMode) {
				// print info and quit:
				LOG.info("The Flink YARN client has been started in detached mode. In order to stop " +
						"Flink on YARN, use the following command or a YARN web interface to stop it:\n" +
						"yarn application -kill " + yarnCluster.getClusterIdentifier() + "\n" +
						"Please also note that the temporary files of the YARN session in {} will not be removed.",
						flinkYarnClient.getSessionFilesDir());
				yarnCluster.disconnect();
			} else {
				runInteractiveCli(yarnCluster, acceptInteractiveInput);

				if (!yarnCluster.hasBeenShutdown()) {
					LOG.info("Command Line Interface requested session shutdown");
					yarnCluster.shutdown();
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
			yarnCluster.shutdown();
		}
	}

	private void logAndSysout(String message) {
		LOG.info(message);
		System.out.println(message);
	}

	public static Map<String, String> getDynamicProperties(String dynamicPropertiesEncoded) {
		if (dynamicPropertiesEncoded != null && dynamicPropertiesEncoded.length() > 0) {
			Map<String, String> properties = new HashMap<>();

			String[] propertyLines = dynamicPropertiesEncoded.split(YARN_DYNAMIC_PROPERTIES_SEPARATOR);
			for (String propLine : propertyLines) {
				if (propLine == null) {
					continue;
				}

				String[] kv = propLine.split("=");
				if (kv.length >= 2 && kv[0] != null && kv[1] != null && kv[0].length() > 0) {
					properties.put(kv[0], kv[1]);
				}
			}
			return properties;
		}
		else {
			return Collections.emptyMap();
		}
	}

	private static String getYarnPropertiesLocation(Configuration conf) {
		String defaultPropertiesFileLocation = System.getProperty("java.io.tmpdir");
		String currentUser = System.getProperty("user.name");
		String propertiesFileLocation = conf.getString(ConfigConstants.YARN_PROPERTIES_FILE_LOCATION, defaultPropertiesFileLocation);

		return propertiesFileLocation + File.separator + YARN_PROPERTIES_FILE + currentUser;
	}
}
