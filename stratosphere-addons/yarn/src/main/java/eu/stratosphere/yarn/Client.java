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


package eu.stratosphere.yarn;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarFile;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import eu.stratosphere.client.CliFrontend;
import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.GlobalConfiguration;

/**
 * All classes in this package contain code taken from
 * https://github.com/apache/hadoop-common/blob/trunk/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-applications/hadoop-yarn-applications-distributedshell/src/main/java/org/apache/hadoop/yarn/applications/distributedshell/Client.java?source=cc
 * and
 * https://github.com/hortonworks/simple-yarn-app
 * and 
 * https://github.com/yahoo/storm-yarn/blob/master/src/main/java/com/yahoo/storm/yarn/StormOnYarn.java
 * 
 * The Stratosphere jar is uploaded to HDFS by this client. 
 * The application master and all the TaskManager containers get the jar file downloaded
 * by YARN into their local fs.
 * 
 */
public class Client {
	private static final Log LOG = LogFactory.getLog(Client.class);
	
	/**
	 * Command Line argument options
	 */
	private static final Option QUERY = new Option("q","query",false, "Display available YARN resources (memory, cores)");
	// --- or ---
	private static final Option VERBOSE = new Option("v","verbose",false, "Verbose debug mode");
	private static final Option GEN_CONF = new Option("g","generateConf",false, "Place default configuration file in current directory");
	private static final Option QUEUE = new Option("qu","queue",true, "Specify YARN queue.");
	private static final Option SHIP_PATH = new Option("s","ship",true, "Ship files in the specified directory");
	private static final Option STRATOSPHERE_CONF_DIR = new Option("c","confDir",true, "Path to Stratosphere configuration directory");
	private static final Option STRATOSPHERE_JAR = new Option("j","jar",true, "Path to Stratosphere jar file");
	private static final Option JM_MEMORY = new Option("jm","jobManagerMemory",true, "Memory for JobManager Container [in MB]");
	private static final Option TM_MEMORY = new Option("tm","taskManagerMemory",true, "Memory per TaskManager Container [in MB]");
	private static final Option TM_CORES = new Option("tmc","taskManagerCores",true, "Virtual CPU cores per TaskManager");
	private static final Option CONTAINER = new Option("n","container",true, "Number of Yarn container to allocate (=Number of"
			+ " TaskTrackers)");
	
	/**
	 * Constants
	 */
	// environment variable names 
	public final static String ENV_TM_MEMORY = "_CLIENT_TM_MEMORY";
	public final static String ENV_TM_CORES = "_CLIENT_TM_CORES";
	public final static String ENV_TM_COUNT = "_CLIENT_TM_COUNT";
	public final static String ENV_APP_ID = "_APP_ID";
	public final static String STRATOSPHERE_JAR_PATH = "_STRATOSPHERE_JAR_PATH"; // the stratosphere jar resource location (in HDFS).
	public static final String ENV_CLIENT_HOME_DIR = "_CLIENT_HOME_DIR";
	public static final String ENV_CLIENT_SHIP_FILES = "_CLIENT_SHIP_FILES";
	public static final String ENV_CLIENT_USERNAME = "_CLIENT_USERNAME";
	
	private static final String CONFIG_FILE_NAME = "stratosphere-conf.yaml";

	
	
	private Configuration conf;

	public void run(String[] args) throws Exception {
		
		if(UserGroupInformation.isSecurityEnabled()) {
			throw new RuntimeException("Stratosphere YARN client does not have security support right now."
					+ "File a bug, we will fix it asap");
		}
		//Utils.logFilesInCurrentDirectory(LOG);
		//
		//	Command Line Options
		//
		Options options = new Options();
		options.addOption(VERBOSE);
		options.addOption(STRATOSPHERE_CONF_DIR);
		options.addOption(STRATOSPHERE_JAR);
		options.addOption(JM_MEMORY);
		options.addOption(TM_MEMORY);
		options.addOption(TM_CORES);
		options.addOption(CONTAINER);
		options.addOption(GEN_CONF);
		options.addOption(QUEUE);
		options.addOption(QUERY);
		options.addOption(SHIP_PATH);
		
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse( options, args);
		} catch(MissingOptionException moe) {
			System.out.println(moe.getMessage());
			printUsage();
			System.exit(1);
		}
		
		if (System.getProperty("log4j.configuration") == null) {
			Logger root = Logger.getRootLogger();
			root.removeAllAppenders();
			PatternLayout layout = new PatternLayout("%d{HH:mm:ss,SSS} %-5p %-60c %x - %m%n");
			ConsoleAppender appender = new ConsoleAppender(layout, "System.err");
			root.addAppender(appender);
			if(cmd.hasOption(VERBOSE.getOpt())) {
				root.setLevel(Level.DEBUG);
				LOG.debug("CLASSPATH: "+System.getProperty("java.class.path"));
			} else {
				root.setLevel(Level.INFO);
			}
		}
		
		
		// Jar Path
		Path localJarPath;
		if(cmd.hasOption(STRATOSPHERE_JAR.getOpt())) {
			String userPath = cmd.getOptionValue(STRATOSPHERE_JAR.getOpt());
			if(!userPath.startsWith("file://")) {
				userPath = "file://" + userPath;
			}
			localJarPath = new Path(userPath);
		} else {
			localJarPath = new Path("file://"+Client.class.getProtectionDomain().getCodeSource().getLocation().getPath());
		}
		
		if(cmd.hasOption(GEN_CONF.getOpt())) {
			LOG.info("Placing default configuration in current directory");
			File outFile = generateDefaultConf(localJarPath);
			LOG.info("File written to "+outFile.getAbsolutePath());
			System.exit(0);
		}
		
		// Conf Path 
		Path confPath = null;
		String confDirPath = "";
		if(cmd.hasOption(STRATOSPHERE_CONF_DIR.getOpt())) {
			confDirPath = cmd.getOptionValue(STRATOSPHERE_CONF_DIR.getOpt())+"/";
			File confFile = new File(confDirPath+CONFIG_FILE_NAME);
			if(!confFile.exists()) {
				LOG.fatal("Unable to locate configuration file in "+confFile);
				System.exit(1);
			}
			confPath = new Path(confFile.getAbsolutePath());
		} else {
			System.out.println("No configuration file has been specified");
			
			// no configuration path given.
			// -> see if there is one in the current directory
			File currDir = new File(".");
			File[] candidates = currDir.listFiles(new FilenameFilter() {
				@Override
				public boolean accept(final File dir, final String name) {
					return name != null && name.endsWith(".yaml");
				}
			});
			if(candidates == null || candidates.length == 0) {
				System.out.println("No configuration file has been found in current directory.\n"
						+ "Copying default.");
				File outFile = generateDefaultConf(localJarPath);
				confPath = new Path(outFile.toURI());
			} else {
				if(candidates.length > 1) {
					System.out.println("Multiple .yaml configuration files were found in the current directory\n"
							+ "Please specify one explicitly");
					System.exit(1);
				} else if(candidates.length == 1) {
					confPath = new Path(candidates[0].toURI());
				} 
			}
		}
		List<File> shipFiles = new ArrayList<File>();
		// path to directory to ship
		if(cmd.hasOption(SHIP_PATH.getOpt())) {
			String shipPath = cmd.getOptionValue(SHIP_PATH.getOpt());
			File shipDir = new File(shipPath);
			if(shipDir.isDirectory()) {
				shipFiles = new ArrayList<File>(Arrays.asList(shipDir.listFiles(new FilenameFilter() {
					@Override
					public boolean accept(File dir, String name) {
						return !(name.equals(".") || name.equals("..") );
					}
				})));
			} else {
				LOG.warn("Ship directory is not a directory!");
			}
		}
		boolean hasLog4j = false;
		//check if there is a log4j file
		if(confDirPath.length() > 0) {
			File l4j = new File(confDirPath+"/log4j.properties");
			if(l4j.exists()) {
				shipFiles.add(l4j);
				hasLog4j = true;
			}
		}
		
		// queue
		String queue = "default";
		if(cmd.hasOption(QUEUE.getOpt())) {
			queue = cmd.getOptionValue(QUEUE.getOpt());
		}
		
		// JobManager Memory
		int jmMemory = 512;
		if(cmd.hasOption(JM_MEMORY.getOpt())) {
			jmMemory = Integer.valueOf(cmd.getOptionValue(JM_MEMORY.getOpt()));
		}
		
		// Task Managers memory
		int tmMemory = 1024;
		if(cmd.hasOption(TM_MEMORY.getOpt())) {
			tmMemory = Integer.valueOf(cmd.getOptionValue(TM_MEMORY.getOpt()));
		}
		
		// Task Managers vcores
		int tmCores = 1;
		if(cmd.hasOption(TM_CORES.getOpt())) {
			tmCores = Integer.valueOf(cmd.getOptionValue(TM_CORES.getOpt()));
		}
		Utils.getStratosphereConfiguration(confPath.toUri().getPath());
		int jmPort = GlobalConfiguration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, 0);
		if(jmPort == 0) {
			LOG.warn("Unable to find job manager port in configuration!");
			jmPort = ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT;
		}
		conf = Utils.initializeYarnConfiguration();
		
		// intialize HDFS
	    LOG.info("Copy App Master jar from local filesystem and add to local environment");
	    // Copy the application master jar to the filesystem 
	    // Create a local resource to point to the destination jar path 
	    final FileSystem fs = FileSystem.get(conf);
	    
	    if(fs.getScheme().startsWith("file")) {
	    	LOG.warn("The file system scheme is '" + fs.getScheme() + "'. This indicates that the "
	    			+ "specified Hadoop configuration path is wrong and the sytem is using the default Hadoop configuration values."
	    			+ "The Stratosphere YARN client needs to store its files in a distributed file system");
	    }
	    
	    // Create yarnClient
		final YarnClient yarnClient = YarnClient.createYarnClient();
		yarnClient.init(conf);
		yarnClient.start();
		
		// Query cluster for metrics
		if(cmd.hasOption(QUERY.getOpt())) {
			showClusterMetrics(yarnClient);
		}
		if(!cmd.hasOption(CONTAINER.getOpt())) {
			LOG.fatal("Missing required argument "+CONTAINER.getOpt());
			printUsage();
			yarnClient.stop();
			System.exit(1);
		}
		
		// TM Count
		final int taskManagerCount = Integer.valueOf(cmd.getOptionValue(CONTAINER.getOpt()));
		
		System.out.println("Using values:");
		System.out.println("\tContainer Count = "+taskManagerCount);
		System.out.println("\tJar Path = "+localJarPath.toUri().getPath());
		System.out.println("\tConfiguration file = "+confPath.toUri().getPath());
		System.out.println("\tJobManager memory = "+jmMemory);
		System.out.println("\tTaskManager memory = "+tmMemory);
		System.out.println("\tTaskManager cores = "+tmCores);

		// Create application via yarnClient
		YarnClientApplication app = yarnClient.createApplication();
		GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
		Resource maxRes = appResponse.getMaximumResourceCapability();
		if(tmMemory > maxRes.getMemory() || tmCores > maxRes.getVirtualCores()) {
			LOG.fatal("The cluster does not have the requested resources for the TaskManagers available!\n"
					+ "Maximum Memory: "+maxRes.getMemory() +", Maximum Cores: "+tmCores);
			yarnClient.stop();
			System.exit(1);
		}
		if(jmMemory > maxRes.getMemory() ) {
			LOG.fatal("The cluster does not have the requested resources for the JobManager available!\n"
					+ "Maximum Memory: "+maxRes.getMemory());
			yarnClient.stop();
			System.exit(1);
		}
		
		// respect custom JVM options in the YAML file
		final String javaOpts = GlobalConfiguration.getString(ConfigConstants.STRATOSPHERE_JVM_OPTIONS, "");
		
		// Set up the container launch context for the application master
		ContainerLaunchContext amContainer = Records
				.newRecord(ContainerLaunchContext.class);
		
		String amCommand = "$JAVA_HOME/bin/java"
					+ " -Xmx"+jmMemory+"M " +javaOpts;
		if(hasLog4j) {
			amCommand 	+= " -Dlog.file=\""+ApplicationConstants.LOG_DIR_EXPANSION_VAR +"/jobmanager-log4j.log\" -Dlog4j.configuration=file:log4j.properties";
		}
		amCommand 	+= " eu.stratosphere.yarn.ApplicationMaster" + " "
					+ " 1>"
					+ ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/jobmanager-stdout.log"
					+ " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/jobmanager-stderr.log";
		amContainer.setCommands(Collections.singletonList(amCommand));
		
		System.err.println("amCommand="+amCommand);
		
		// Set-up ApplicationSubmissionContext for the application
		ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
		final ApplicationId appId = appContext.getApplicationId();
		
		// Setup jar for ApplicationMaster
		LocalResource appMasterJar = Records.newRecord(LocalResource.class);
		LocalResource stratosphereConf = Records.newRecord(LocalResource.class);
		Path remotePathJar = Utils.setupLocalResource(conf, fs, appId.toString(), localJarPath, appMasterJar, fs.getHomeDirectory());
		Path remotePathConf = Utils.setupLocalResource(conf, fs, appId.toString(), confPath, stratosphereConf, fs.getHomeDirectory());
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>(2);
		localResources.put("stratosphere.jar", appMasterJar);
		localResources.put("stratosphere-conf.yaml", stratosphereConf);
		
		
		// setup security tokens (code from apache storm)
		final Path[] paths = new Path[3 + shipFiles.size()];
		StringBuffer envShipFileList = new StringBuffer();
		// upload ship files
		for (int i = 0; i < shipFiles.size(); i++) {
			File shipFile = shipFiles.get(i);
			LocalResource shipResources = Records.newRecord(LocalResource.class);
			Path shipLocalPath = new Path("file://" + shipFile.getAbsolutePath());
			paths[3 + i] = Utils.setupLocalResource(conf, fs, appId.toString(),
					shipLocalPath, shipResources, fs.getHomeDirectory());
			localResources.put(shipFile.getName(), shipResources);
			
			envShipFileList.append(paths[3 + i]);
			if(i+1 < shipFiles.size()) {
				envShipFileList.append(',');
			}
		}

		paths[0] = remotePathJar;
		paths[1] = remotePathConf;
		paths[2] = new Path(fs.getHomeDirectory(), ".stratosphere/" + appId.toString() + "/");
		FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
		fs.setPermission(paths[2], permission); // set permission for path.
		Utils.setTokensFor(amContainer, paths, this.conf);
		
         
		amContainer.setLocalResources(localResources);
		fs.close();

		// Setup CLASSPATH for ApplicationMaster
		Map<String, String> appMasterEnv = new HashMap<String, String>();
		Utils.setupEnv(conf, appMasterEnv);
		// set configuration values
		appMasterEnv.put(Client.ENV_TM_COUNT, String.valueOf(taskManagerCount));
		appMasterEnv.put(Client.ENV_TM_CORES, String.valueOf(tmCores));
		appMasterEnv.put(Client.ENV_TM_MEMORY, String.valueOf(tmMemory));
		appMasterEnv.put(Client.STRATOSPHERE_JAR_PATH, remotePathJar.toString() );
		appMasterEnv.put(Client.ENV_APP_ID, appId.toString());
		appMasterEnv.put(Client.ENV_CLIENT_HOME_DIR, fs.getHomeDirectory().toString());
		appMasterEnv.put(Client.ENV_CLIENT_SHIP_FILES, envShipFileList.toString() );
		appMasterEnv.put(Client.ENV_CLIENT_USERNAME, UserGroupInformation.getCurrentUser().getShortUserName());
		
		amContainer.setEnvironment(appMasterEnv);
		
		// Set up resource type requirements for ApplicationMaster
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(jmMemory);
		capability.setVirtualCores(1);
		
		appContext.setApplicationName("Stratosphere"); // application name
		appContext.setAMContainerSpec(amContainer);
		appContext.setResource(capability);
		appContext.setQueue(queue);
		
		// file that we write into the conf/ dir containing the jobManager address.
		final File addrFile = new File(confDirPath + CliFrontend.JOBMANAGER_ADDRESS_FILE);
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
		   @Override
		   public void run() {
		    try {
		    	LOG.info("Killing the Stratosphere-YARN application.");
				yarnClient.killApplication(appId);
				LOG.info("Deleting files in "+paths[2]);
				FileSystem shutFS = FileSystem.get(conf);
				shutFS.delete(paths[2], true); // delete conf and jar file.
				shutFS.close();
			} catch (Exception e) {
				LOG.warn("Exception while killing the YARN application", e);
			}
		    try {
		    	addrFile.delete();
		    } catch (Exception e) {
		    	LOG.warn("Exception while deleting the jobmanager address file", e);
		    }
		    LOG.info("YARN Client is shutting down");
		    yarnClient.stop();
		   }
		  });
		
		LOG.info("Submitting application master " + appId);
		yarnClient.submitApplication(appContext);
		ApplicationReport appReport = yarnClient.getApplicationReport(appId);
		YarnApplicationState appState = appReport.getYarnApplicationState();
		boolean told = false;
		char[] el = { '/', '|', '\\', '-'};
		int i = 0; 
		while (appState != YarnApplicationState.FINISHED
				&& appState != YarnApplicationState.KILLED
				&& appState != YarnApplicationState.FAILED) {
			if(!told && appState ==  YarnApplicationState.RUNNING) {
				System.err.println("Stratosphere JobManager is now running on "+appReport.getHost()+":"+jmPort);
				System.err.println("JobManager Web Interface: "+appReport.getTrackingUrl());
				// write jobmanager connect information
				
				PrintWriter out = new PrintWriter(addrFile);
				out.println(appReport.getHost()+":"+jmPort);
				out.close();
				addrFile.setReadable(true, false); // readable for all.
				told = true;
			}
			if(!told) {
				System.err.print(el[i++]+"\r");
				if(i == el.length) i = 0;
				Thread.sleep(500); // wait for the application to switch to RUNNING
			} else {
				Thread.sleep(5000);
			}
			
			appReport = yarnClient.getApplicationReport(appId);
			appState = appReport.getYarnApplicationState();
		}

		LOG.info("Application " + appId + " finished with"
				+ " state " + appState + " at " + appReport.getFinishTime());
		if(appState == YarnApplicationState.FAILED || appState == YarnApplicationState.KILLED ) {
			LOG.warn("Application failed. Diagnostics "+appReport.getDiagnostics());
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
		opt.addOption(VERBOSE);
	//	opt.addOption(GEN_CONF);
	//	opt.addOption(STRATOSPHERE_CONF);
	//	opt.addOption(STRATOSPHERE_JAR);
		opt.addOption(JM_MEMORY);
		opt.addOption(TM_MEMORY);
		opt.addOption(TM_CORES);
		opt.addOption(QUERY);
		opt.addOption(QUEUE);
		formatter.printHelp(" ", opt);
	}

	private void showClusterMetrics(YarnClient yarnClient)
			throws YarnException, IOException {
		YarnClusterMetrics metrics = yarnClient.getYarnClusterMetrics();
		System.out.println("NodeManagers in Cluster " + metrics.getNumNodeManagers());
		List<NodeReport> nodes = yarnClient.getNodeReports();
		final String format = "|%-16s |%-16s %n";
		System.out.printf("|Property         |Value          %n");
		System.out.println("+---------------------------------------+");
		int totalMemory = 0;
		int totalCores = 0;
		for(NodeReport rep : nodes) {
			final Resource res = rep.getCapability();
			totalMemory += res.getMemory();
			totalCores += res.getVirtualCores();
			System.out.format(format, "NodeID", rep.getNodeId());
			System.out.format(format, "Memory", res.getMemory()+" MB");
			System.out.format(format, "vCores", res.getVirtualCores());
			System.out.format(format, "HealthReport", rep.getHealthReport());
			System.out.format(format, "Containers", rep.getNumContainers());
			System.out.println("+---------------------------------------+");
		}
		System.out.println("Summary: totalMemory "+totalMemory+" totalCores "+totalCores);
		List<QueueInfo> qInfo = yarnClient.getAllQueues();
		for(QueueInfo q : qInfo) {
			System.out.println("Queue: "+q.getQueueName()+", Current Capacity: "+q.getCurrentCapacity()+" Max Capacity: "+q.getMaximumCapacity()+" Applications: "+q.getApplications().size());
		}
		yarnClient.stop();
		System.exit(0);
	}

	private File generateDefaultConf(Path localJarPath) throws IOException,
			FileNotFoundException {
		JarFile jar = null;
		try {
			jar = new JarFile(localJarPath.toUri().getPath());
		} catch(FileNotFoundException fne) {
			LOG.fatal("Unable to access jar file. Specify jar file or configuration file.", fne);
			System.exit(1);
		}
		InputStream confStream = jar.getInputStream(jar.getEntry("stratosphere-conf.yaml"));
		
		if(confStream == null) {
			LOG.warn("Given jar file does not contain yaml conf.");
			confStream = this.getClass().getResourceAsStream("stratosphere-conf.yaml"); 
			if(confStream == null) {
				throw new RuntimeException("Unable to find stratosphere-conf in jar file");
			}
		}
		File outFile = new File("stratosphere-conf.yaml");
		if(outFile.exists()) {
			throw new RuntimeException("File unexpectedly exists");
		}
		FileOutputStream outputStream = new FileOutputStream(outFile);
		int read = 0;
		byte[] bytes = new byte[1024];
		while ((read = confStream.read(bytes)) != -1) {
			outputStream.write(bytes, 0, read);
		}
		confStream.close(); outputStream.close(); jar.close();
		return outFile;
	}

	public static void main(String[] args) throws Exception {
		Client c = new Client();
		c.run(args);
	}
}
