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

package org.apache.flink.mesos;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;

public class MesosController {
	/**
	 * Command Line argument options
	 */
	private static final Option QUERY = new Option("q","query",false, "Display available YARN resources (memory, cores)");
	// --- or ---
	private static final Option VERBOSE = new Option("v","verbose",false, "Verbose debug mode");
	private static final Option QUEUE = new Option("qu","queue",true, "Specify YARN queue.");
	private static final Option FLINK_CONF_DIR = new Option("c","confDir",true, "Path to Flink configuration directory");
	private static final Option FLINK_JAR = new Option("j","jar",true, "Path to Flink jar file");
	private static final Option JM_MEMORY = new Option("jm","jobManagerMemory",true, "Memory for JobManager Container [in MB]");
	private static final Option TM_MEMORY = new Option("tm","taskManagerMemory",true, "Memory per TaskManager Container [in MB]");
	private static final Option TM_CORES = new Option("tmc","taskManagerCores",true, "Virtual CPU cores per TaskManager");
	private static final Option NUM_TM = new Option("n","container",true, "Number of Task Managers, greedy behaviour if not specified");
	private static final Option SLOTS = new Option("s","slots",true, "Number of slots per TaskManager");
	private static final Option MASTER = new Option("m","master",true, "Address of the Mesos master node");

	private void printUsage() {
		System.out.println("Usage:");
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(200);
		formatter.setLeftPadding(5);
		formatter.setSyntaxPrefix("   Required");
		Options req = new Options();
		req.addOption(FLINK_JAR);
		req.addOption(FLINK_CONF_DIR);
		req.addOption(MASTER);

		formatter.printHelp(" ", req);

		formatter.setSyntaxPrefix("   Optional");
		Options opt = new Options();
		opt.addOption(NUM_TM);
		opt.addOption(VERBOSE);
		opt.addOption(JM_MEMORY);
		opt.addOption(TM_MEMORY);
		opt.addOption(TM_CORES);
		opt.addOption(QUERY);
		opt.addOption(QUEUE);
		opt.addOption(SLOTS);
		formatter.printHelp(" ", opt);
	}

	public static void addDir(String s) throws IOException {
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

	public void run(String[] args) throws Exception {
		addDir("/home/sebastian/IdeaProjects/mesos-0.20.1/build/src/.libs/");
		Options options = new Options();
		options.addOption(VERBOSE);
		options.addOption(FLINK_CONF_DIR);
		options.addOption(FLINK_JAR);
		options.addOption(JM_MEMORY);
		options.addOption(TM_MEMORY);
		options.addOption(TM_CORES);
		options.addOption(QUEUE);
		options.addOption(QUERY);
		options.addOption(SLOTS);
		options.addOption(MASTER);

		System.out.println(System.getProperty("java.library.path"));
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse( options, args);
		} catch(MissingOptionException moe) {
			System.out.println(moe.getMessage());
			printUsage();
			System.exit(1);
		}
		String jarPath;
		if(cmd.hasOption(FLINK_JAR.getOpt())) {
			jarPath = cmd.getOptionValue(FLINK_JAR.getOpt());
		} else {
			jarPath = "file://" + MesosController.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		}

		String flinkConfDir = null;
		if(cmd.hasOption(FLINK_CONF_DIR.getOpt())) {
			flinkConfDir = cmd.getOptionValue(FLINK_CONF_DIR.getOpt());
		} else {
			printUsage();
			System.exit(1);
		}

		String mesosMaster = null;
		if(cmd.hasOption(MASTER.getOpt())) {
			mesosMaster = cmd.getOptionValue(MASTER.getOpt());
		} else {
			printUsage();
			System.exit(1);
		}

		Protos.FrameworkInfo framework = Protos.FrameworkInfo.newBuilder()
				.setUser("") // Have Mesos fill in the current user.
				.setName("Flink Test")
				.setPrincipal("Flink")
				.build();

		MesosSchedulerDriver driver = new MesosSchedulerDriver(
				new FlinkMesosScheduler(jarPath, flinkConfDir),
				framework,
				mesosMaster);

		Protos.Status result = driver.run();
		System.exit(result.getNumber());
	}

	public static void main(String[] args) throws Exception {
		MesosController mesosController = new MesosController();
		mesosController.run(args);
	}
}
