/***********************************************************************************************************************
 *
 * Copyright (C) 2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

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



package eu.stratosphere.yarn;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.InputStreamReader;
import java.io.Writer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.util.Records;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.jobmanager.JobManager;

public class ApplicationMaster {

	private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);
	
	public static class JobManagerRunner extends Thread {
		private String pathToNepheleConfig = "";
		private JobManager jm;
		
		public JobManagerRunner(String pathToNepheleConfig) {
			super("Job manager runner");
			this.pathToNepheleConfig = pathToNepheleConfig;
		}

		public void run() {
			String[] args = {"-executionMode","cluster", "-configDir", pathToNepheleConfig};
			this.jm = JobManager.initialize( args );
			
			// Start info server for jobmanager
			//jobManager.startInfoServer();

			// Run the main task loop
			this.jm.runTaskLoop();
		}
		public void shutdown() {
			this.jm.shutdown();
		}
	}
	public static void main(String[] args) throws Exception {
		// LOG.info("CLASSPATH: "+System.getProperty("java.class.path"));
//		Map<String, String> env = System.getenv();
//        for (String envName : env.keySet()) {
//        	LOG.info(envName+"="+env.get(envName));
//        }
		
		// Initialize clients to ResourceManager and NodeManagers
		Configuration conf = Utils.initializeYarnConfiguration();
		FileSystem fs = FileSystem.get(conf);
		Map<String, String> envs = System.getenv();
		final String currDir = envs.get(Environment.PWD.key());
		final String ownHostname = envs.get(Environment.NM_HOST.key());
		int appId = Integer.valueOf(envs.get(Client.ENV_APP_ID));
		final String localDirs = envs.get(Environment.LOCAL_DIRS.key());
		final String applicationMasterHost = envs.get(Environment.NM_HOST.key());
		final String remoteStratosphereJarPath = envs.get(Client.STRATOSPHERE_JAR_PATH);
		final int taskManagerCount = Integer.valueOf(envs.get(Client.ENV_TM_COUNT));
		final int memoryPerTaskManager = Integer.valueOf(envs.get(Client.ENV_TM_MEMORY));
		final int coresPerTaskManager = Integer.valueOf(envs.get(Client.ENV_TM_CORES));
		
		final int heapLimit = (int)((float)memoryPerTaskManager*0.7);
		
		if(currDir == null) throw new RuntimeException("Current directory unknown");
		if(ownHostname == null) throw new RuntimeException("Own hostname ("+Environment.NM_HOST+") not set.");
		LOG.info("Working directory "+currDir);
		
		// Update yaml conf -> set jobManager address to this machine's address.
		// (I never know how to nicely do file i/o in java.)
		FileInputStream fis = new FileInputStream(currDir+"/stratosphere-conf.yaml");
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));
		Writer output = new BufferedWriter(new FileWriter(currDir+"/stratosphere-conf-modified.yaml"));
		String line ;
		while ( (line = br.readLine()) != null) {
		    if(line.contains(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY)) {
		    	output.append(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY+": "+ownHostname+"\n");
		    } else if(localDirs != null && line.contains(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY)) {
		    	output.append(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY+": "+localDirs+"\n");
		    } else {
		    	output.append(line+"\n");
		    }
		}
		output.close();
		br.close();
		LOG.info("Wrote to: "+currDir+"/stratosphere-conf-modified.yaml");
		File newConf = new File(currDir+"/stratosphere-conf-modified.yaml");
		if(!newConf.exists()) {
			LOG.warn("modified yaml does not exist!");
		}
		// list current files.
		File cd = new File(".");
		File[] candidates = cd.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(final File dir, final String name) {
				LOG.info("Files in this directory: "+name);
				return name != null && name.endsWith(".yaml");
			}
		});
		
		// TODO: Copy web frontend files.
		
		JobManagerRunner jmr = new JobManagerRunner(currDir+"/stratosphere-conf-modified.yaml");
		LOG.info("Starting JobManager");
		jmr.start();
		
		AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
		rmClient.init(conf);
		rmClient.start();

		NMClient nmClient = NMClient.createNMClient();
		nmClient.init(conf);
		nmClient.start();

		// Register with ResourceManager
		LOG.info("registering ApplicationMaster");
		rmClient.registerApplicationMaster(applicationMasterHost, 0, "http://"+applicationMasterHost+":"+GlobalConfiguration.getString(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, "undefined"));

		// Priority for worker containers - priorities are intra-application
		Priority priority = Records.newRecord(Priority.class);
		priority.setPriority(0);

		// Resource requirements for worker containers
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(memoryPerTaskManager);
		capability.setVirtualCores(coresPerTaskManager);

		// Make container requests to ResourceManager
		for (int i = 0; i < taskManagerCount; ++i) {
			ContainerRequest containerAsk = new ContainerRequest(capability,
					null, null, priority);
			LOG.info("Requesting TaskManager container " + i);
			rmClient.addContainerRequest(containerAsk);
		}

		
		
		LocalResource stratosphereJar = Records.newRecord(LocalResource.class);
		LocalResource stratosphereConf = Records.newRecord(LocalResource.class);

		// register Stratosphere Jar with remote HDFS
		final Path remoteJarPath = new Path(remoteStratosphereJarPath);
		Utils.registerLocalResource(fs, remoteJarPath, stratosphereJar);
	//	Utils.setupLocalResource(conf, fs, appId, new Path("file://"+currDir+"/stratosphere.jar"), stratosphereJar);
		
		// register conf with local fs.
		Utils.setupLocalResource(conf, fs, appId, new Path("file://"+currDir+"/stratosphere-conf-modified.yaml"), stratosphereConf);
		LOG.info("Prepared localresource for modified yaml: "+stratosphereConf);
		
		// Obtain allocated containers and launch
		int allocatedContainers = 0;
		int completedContainers = 0;
		while (allocatedContainers < taskManagerCount) {
			AllocateResponse response = rmClient.allocate(0);
			for (Container container : response.getAllocatedContainers()) {
				++allocatedContainers;

				// Launch container by create ContainerLaunchContext
				ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
				
				String tmCommand = "$JAVA_HOME/bin/java -Xmx"+heapLimit+"m " 
						+ " eu.stratosphere.nephele.taskmanager.TaskManager -configDir . "
						+ " 1>"
						+ ApplicationConstants.LOG_DIR_EXPANSION_VAR
						+ "/stdout" 
						+ " 2>"
						+ ApplicationConstants.LOG_DIR_EXPANSION_VAR
						+ "/stderr";
				ctx.setCommands(Collections.singletonList(tmCommand));
				
				LOG.info("Starting TM with command="+tmCommand);
				
				// copy resources to the TaskManagers.
				Map<String, LocalResource> localResources = new HashMap<String, LocalResource>(2);
				localResources.put("stratosphere.jar", stratosphereJar);
				localResources.put("stratosphere-conf.yaml", stratosphereConf);
				
				ctx.setLocalResources(localResources);
				
				// Setup CLASSPATH for Container (=TaskTracker)
				Map<String, String> containerEnv = new HashMap<String, String>();
				Utils.setupEnv(conf, containerEnv); //add stratosphere.jar to class path.
				ctx.setEnvironment(containerEnv);
				
				LOG.info("Launching container " + allocatedContainers);
				nmClient.startContainer(container, ctx);
			}
			for (ContainerStatus status : response.getCompletedContainersStatuses()) {
				++completedContainers;
				LOG.info("Completed container "+status.getContainerId()+". Total Completed:" + completedContainers);
			}
			Thread.sleep(100);
		}

		// Now wait for containers to complete
		
		while (completedContainers < taskManagerCount) {
			AllocateResponse response = rmClient.allocate(completedContainers
					/ taskManagerCount);
			for (ContainerStatus status : response.getCompletedContainersStatuses()) {
				++completedContainers;
				LOG.info("Completed container "+status.getContainerId()+". Total Completed:" + completedContainers);
			}
			Thread.sleep(5000);
		}
		
		jmr.shutdown();
		jmr.join(500);
		
		// Un-register with ResourceManager
		rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
	}
}
