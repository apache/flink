/**
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

package org.apache.flink.mesos.core;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.mesos.utility.MesosConfiguration;
import org.apache.flink.mesos.utility.MesosConstants;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;

/**
 * This class parses the command line options given to it and saves it into a Configuration. This configuration is
 * handed to the MesosScheduler class which is responsible for launching the JobManagers and TaskManagers on the available
 * Mesos nodes.
 */
public class MesosController {
	private static final Logger LOG = LoggerFactory.getLogger(MesosController.class);

	/*
		These are the possible command line options.
    */
	private static final Option VERBOSE = new Option("v","verbose",false, "Verbose debug mode");
	private static final Option FLINK_CONF_DIR = new Option("c","confDir",true, "Path to Flink configuration directory");
	private static final Option FLINK_JAR = new Option("j","jar",true, "Path to Flink jar file");
	private static final Option JM_MEMORY = new Option("jm","jobManagerMemory",true, "Memory for JobManager Container [in MB]");
	private static final Option TM_MEMORY = new Option("tm","taskManagerMemory",true, "Memory per TaskManager Container [in MB]");
	private static final Option TM_CORES = new Option("tmc","taskManagerCores",true, "Maximum CPU cores per TaskManager.");
	private static final Option NUM_TM = new Option("n","container",true, "Number of Task Managers, greedy behaviour if not specified");
	private static final Option SLOTS = new Option("s","slots",true, "Number of slots per TaskManager");
	private static final Option MASTER = new Option("m","master",true, "Address of the Mesos master node");
	private static final Option MESOS_LIB = new Option("l","lib",true, "Path to Mesos library files");
	private static final Option USE_WEB = new Option("w","web",false, "Launch the web frontend on the jobmanager node.");
	private static final Option JM_CORES = new Option("jmc","jobManagerCores",true, "Number of Jobmanager Cores");
	private static final Option HELP = new Option("h","help",false, "print help");
	private static final Option SINGLENODE = new Option("sn","singlenode",false, "Allow job- and taskmanager on same node");
	/**
	 * Dynamic properties allow the user to specify additional configuration values with -D, such as
	 *  -Dfs.overwrite-files=true  -Dtaskmanager.network.numberOfBuffers=16368
	 */
	private static final Option DYNAMIC_PROPERTIES = new Option("D", true, "Dynamic properties");


	/**
	 * Prints the required and optional parameters to avoid user mistakes.
	 */
	private void printUsage() {
		System.out.println("Usage:");
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(200);
		formatter.setLeftPadding(5);
		formatter.setSyntaxPrefix("   Required");
		Options req = new Options();
		req.addOption(MASTER);
		req.addOption(MESOS_LIB);
		req.addOption(FLINK_JAR);
		formatter.printHelp(" ", req);

		formatter.setSyntaxPrefix("   Optional");
		Options opt = new Options();
		opt.addOption(FLINK_CONF_DIR);
		opt.addOption(HELP);
		opt.addOption(NUM_TM);
		opt.addOption(VERBOSE);
		opt.addOption(JM_MEMORY);
		opt.addOption(TM_MEMORY);
		opt.addOption(TM_CORES);
		opt.addOption(JM_CORES);
		opt.addOption(SLOTS);
		opt.addOption(USE_WEB);
		opt.addOption(SINGLENODE);
		opt.addOption(DYNAMIC_PROPERTIES);
		formatter.printHelp(" ", opt);
	}

	/**
	 * This is a workaround to modify the java.library.path at runtime. It is necessary to achieve
	 * a cleaner command line interface in combination with scripting so that it is possible to forward all
	 * the command line options given to a script to the java program.
	 * @param s Path to add to java.library.path
	 * @throws IOException
	 */
	public static void addPathToLibrary(String s) throws IOException {
		try {
			// This enables the java.library.path to be modified at runtime
			// From a Sun engineer at http://forums.sun.com/thread.jspa?threadID=707176
			//
			Field field = ClassLoader.class.getDeclaredField("usr_paths");
			field.setAccessible(true);
			String[] paths = (String[]) field.get(null);
			for (int i = 0; i < paths.length; i++) {
				if (s.equals(paths[i])) {
					return;
				}
			}
			String[] tmp = new String[paths.length+1];
			System.arraycopy(paths,0,tmp,0,paths.length);
			tmp[paths.length] = s;
			field.set(null,tmp);
			System.setProperty("java.library.path", System.getProperty("java.library.path") + File.pathSeparator + s);
		} catch (IllegalAccessException e) {
			throw new IOException("Failed to get permissions to set library path");
		} catch (NoSuchFieldException e) {
			throw new IOException("Failed to get field handle to set library path");
		}
	}

	/**
	 * Main work is performed here. (see class description above)
	 * @param args Command Line Arguments
	 * @throws Exception
	 */
	public void run(String[] args) throws Exception {

		Options options = new Options();
		options.addOption(VERBOSE);
		options.addOption(FLINK_CONF_DIR);
		options.addOption(FLINK_JAR);
		options.addOption(JM_MEMORY);
		options.addOption(TM_MEMORY);
		options.addOption(NUM_TM);
		options.addOption(TM_CORES);
		options.addOption(JM_CORES);
		options.addOption(SLOTS);
		options.addOption(MASTER);
		options.addOption(MESOS_LIB);
		options.addOption(USE_WEB);
		options.addOption(SINGLENODE);
		options.addOption(DYNAMIC_PROPERTIES);

		LOG.info("starting");
		MesosConfiguration config = new MesosConfiguration();
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse( options, args);
		} catch(MissingOptionException moe) {
			printUsage();
			System.exit(1);
		} catch (MissingArgumentException mae) {
			printUsage();
			System.exit(1);
		}

		if (cmd.hasOption(HELP.getOpt())) {
			printUsage();
			System.exit(1);
		}

		/*
		The path to the native mesos library is required since it is written in C++.
		 */
		if (cmd.hasOption(MESOS_LIB.getOpt())) {
			String mesosLib = cmd.getOptionValue(MESOS_LIB.getOpt());
			addPathToLibrary(mesosLib);
			config.setString(MesosConstants.MESOS_LIB, mesosLib);
		} else {
			printUsage();
			System.exit(1);
		}

		String jarPath;
		if(cmd.hasOption(FLINK_JAR.getOpt())) {
			jarPath = cmd.getOptionValue(FLINK_JAR.getOpt());
		} else {
			jarPath = "file://" + MesosController.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		}
		config.setString(MesosConstants.MESOS_UBERJAR_LOCATION, jarPath);

		String flinkConfDir = null;
		if (cmd.hasOption(FLINK_CONF_DIR.getOpt())) {
			flinkConfDir = cmd.getOptionValue(FLINK_CONF_DIR.getOpt());
			config.setString(MesosConstants.MESOS_CONF_DIR, flinkConfDir);
		}

		String mesosMaster = null;
		if (cmd.hasOption(MASTER.getOpt())) {
			mesosMaster = cmd.getOptionValue(MASTER.getOpt());
			config.setString(MesosConstants.MESOS_MASTER, mesosMaster);
		} else {
			printUsage();
			System.exit(1);
		}

		if (cmd.hasOption(TM_CORES.getOpt())) {
			config.setInteger(MesosConstants.MESOS_TASK_MANAGER_CORES, new Integer(cmd.getOptionValue(TM_CORES.getOpt())));
		}
		if (cmd.hasOption(JM_MEMORY.getOpt())) {
			config.setDouble(MesosConstants.MESOS_JOB_MANAGER_MEMORY, new Double(cmd.getOptionValue(JM_MEMORY.getOpt())));
		}
		if (cmd.hasOption(JM_CORES.getOpt())) {
			config.setDouble(MesosConstants.MESOS_JOB_MANAGER_CORES, new Double(cmd.getOptionValue(JM_CORES.getOpt())));
		}
		if (cmd.hasOption(TM_MEMORY.getOpt())) {
			config.setDouble(MesosConstants.MESOS_TASK_MANAGER_MEMORY, new Double(cmd.getOptionValue(TM_MEMORY.getOpt())));
		}
		if (cmd.hasOption(SLOTS.getOpt())) {
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, new Integer(cmd.getOptionValue(SLOTS.getOpt())));
		}
		if (cmd.hasOption(NUM_TM.getOpt())) {
			config.setInteger(MesosConstants.MESOS_MAX_TM_INSTANCES, new Integer(cmd.getOptionValue(NUM_TM.getOpt())));
		}

		String[] dynamicProperties = null;
		if(cmd.hasOption(DYNAMIC_PROPERTIES.getOpt())) {
			dynamicProperties = cmd.getOptionValues(DYNAMIC_PROPERTIES.getOpt());
			for (String property: dynamicProperties) {
				String[] tokens = property.split("=");
				if (tokens.length != 2) {
					System.out.println("Error parsing dynamic Properties. They should have the form\nidentifier=value");
					printUsage();
					System.exit(1);
				}
				System.out.println(tokens[0] + ": " + tokens[1]);
				config.setString(tokens[0], tokens[1]);
			}
		}

		/*
		This FrameworkInfo object represents the whole flink "application" on mesos.
		 */
		Protos.FrameworkInfo framework = Protos.FrameworkInfo.newBuilder()
				.setUser("") // Have Mesos fill in the current user.
				.setName("Flink Test")
				.setPrincipal("Flink")
				.build();


		if (cmd.hasOption(USE_WEB.getOpt())) {
			config.setBoolean(MesosConstants.MESOS_USE_WEB, true);
		}

		if (cmd.hasOption(SINGLENODE.getOpt())) {
			config.setBoolean(MesosConstants.SINGLENODE, true);
		}

		MesosSchedulerDriver driver = new MesosSchedulerDriver(
				new FlinkMesosScheduler(config),
				framework,
				config.getString(MesosConstants.MESOS_MASTER, null));

		Protos.Status result = driver.run();
		System.exit(result.getNumber());
	}

	public static void main(String[] args) throws Exception {
		MesosController mesosController = new MesosController();
		mesosController.run(args);
	}
}
