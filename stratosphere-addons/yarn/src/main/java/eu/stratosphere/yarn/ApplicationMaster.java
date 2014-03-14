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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
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

import com.google.common.base.Preconditions;

import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.GlobalConfiguration;
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
			this.jm.startInfoServer();

			// Run the main task loop
			this.jm.runTaskLoop();
		}
		public void shutdown() {
			this.jm.shutdown();
		}
	}
	
	private void run() throws Exception  {
		//Utils.logFilesInCurrentDirectory(LOG);
		// Initialize clients to ResourceManager and NodeManagers
		Configuration conf = Utils.initializeYarnConfiguration();
		FileSystem fs = FileSystem.get(conf);
		Map<String, String> envs = System.getenv();
		final String currDir = envs.get(Environment.PWD.key());
		final String logDirs =  envs.get(Environment.LOG_DIRS.key());
		final String ownHostname = envs.get(Environment.NM_HOST.key());
		final String appId = envs.get(Client.ENV_APP_ID);
		final String localDirs = envs.get(Environment.LOCAL_DIRS.key());
		final String clientHomeDir = envs.get(Client.ENV_CLIENT_HOME_DIR);
		final String applicationMasterHost = envs.get(Environment.NM_HOST.key());
		final String remoteStratosphereJarPath = envs.get(Client.STRATOSPHERE_JAR_PATH);
		final String shipListString = envs.get(Client.ENV_CLIENT_SHIP_FILES);
		final String yarnClientUsername = envs.get(Client.ENV_CLIENT_USERNAME);
		final int taskManagerCount = Integer.valueOf(envs.get(Client.ENV_TM_COUNT));
		final int memoryPerTaskManager = Integer.valueOf(envs.get(Client.ENV_TM_MEMORY));
		final int coresPerTaskManager = Integer.valueOf(envs.get(Client.ENV_TM_CORES));
		
		final int heapLimit = (int)((float)memoryPerTaskManager*0.7);
		
		if(currDir == null) {
			throw new RuntimeException("Current directory unknown");
		}
		if(ownHostname == null) {
			throw new RuntimeException("Own hostname ("+Environment.NM_HOST+") not set.");
		}
		LOG.info("Working directory "+currDir);
		
		// load Stratosphere configuration.
		Utils.getStratosphereConfiguration(currDir);
		
		final String localWebInterfaceDir = currDir+"/resources/"+ConfigConstants.DEFAULT_JOB_MANAGER_WEB_PATH_NAME;
		
		// Update yaml conf -> set jobManager address to this machine's address.
		FileInputStream fis = new FileInputStream(currDir+"/stratosphere-conf.yaml");
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));
		Writer output = new BufferedWriter(new FileWriter(currDir+"/stratosphere-conf-modified.yaml"));
		String line ;
		while ( (line = br.readLine()) != null) {
		    if(line.contains(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY)) {
		    	output.append(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY+": "+ownHostname+"\n");
		    } else if(line.contains(ConfigConstants.JOB_MANAGER_WEB_ROOT_PATH_KEY)) {
		    	output.append(ConfigConstants.JOB_MANAGER_WEB_ROOT_PATH_KEY+": "+"\n");
		    } else if(localDirs != null && line.contains(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY)) {
		    	output.append(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY+": "+localDirs+"\n");
		    } else {
		    	output.append(line+"\n");
		    }
		}
		// just to make sure.
		output.append(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY+": "+ownHostname+"\n");
		output.append(ConfigConstants.JOB_MANAGER_WEB_ROOT_PATH_KEY+": "+localWebInterfaceDir+"\n");
		output.append(ConfigConstants.JOB_MANAGER_WEB_LOG_PATH_KEY+": "+logDirs+"\n");
		if(localDirs != null) {
			output.append(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY+": "+localDirs+"\n");
		}
		output.close();
		br.close();
		File newConf = new File(currDir+"/stratosphere-conf-modified.yaml");
		if(!newConf.exists()) {
			LOG.warn("modified yaml does not exist!");
		}
		
		Utils.copyJarContents("resources/"+ConfigConstants.DEFAULT_JOB_MANAGER_WEB_PATH_NAME, 
				ApplicationMaster.class.getProtectionDomain().getCodeSource().getLocation().getPath());
		
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
		
		// register conf with local fs.
		Path remoteConfPath = Utils.setupLocalResource(conf, fs, appId, new Path("file://"+currDir+"/stratosphere-conf-modified.yaml"), stratosphereConf, new Path(clientHomeDir));
		LOG.info("Prepared localresource for modified yaml: "+stratosphereConf);
		
		
		boolean hasLog4j = new File(currDir+"/log4j.properties").exists();
		// prepare the files to ship
		LocalResource[] remoteShipRsc = null;
		String[] remoteShipPaths = shipListString.split(",");
		if(!shipListString.isEmpty()) {
			remoteShipRsc = new LocalResource[remoteShipPaths.length]; 
			{ // scope for i
				int i = 0;
				for(String remoteShipPathStr : remoteShipPaths) {
					if(remoteShipPathStr == null || remoteShipPathStr.isEmpty()) {
						continue;
					}
					remoteShipRsc[i] = Records.newRecord(LocalResource.class);
					Path remoteShipPath = new Path(remoteShipPathStr);
					Utils.registerLocalResource(fs, remoteShipPath, remoteShipRsc[i]);
					i++;
				}
			}
		}
		
		// respect custom JVM options in the YAML file
		final String javaOpts = GlobalConfiguration.getString(ConfigConstants.STRATOSPHERE_JVM_OPTIONS, "");
				
		// Obtain allocated containers and launch
		int allocatedContainers = 0;
		int completedContainers = 0;
		while (allocatedContainers < taskManagerCount) {
			AllocateResponse response = rmClient.allocate(0);
			for (Container container : response.getAllocatedContainers()) {
				LOG.info("Got new Container for TM "+container.getId()+" on host "+container.getNodeId().getHost());
				++allocatedContainers;

				// Launch container by create ContainerLaunchContext
				ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
				
				String tmCommand = "$JAVA_HOME/bin/java -Xmx"+heapLimit+"m " + javaOpts ;
				if(hasLog4j) {
					tmCommand += " -Dlog.file=\""+ApplicationConstants.LOG_DIR_EXPANSION_VAR +"/taskmanager-log4j.log\" -Dlog4j.configuration=file:log4j.properties";
				}
				tmCommand	+= " eu.stratosphere.yarn.YarnTaskManagerRunner -configDir . "
						+ " 1>"
						+ ApplicationConstants.LOG_DIR_EXPANSION_VAR
						+ "/taskmanager-stdout.log" 
						+ " 2>"
						+ ApplicationConstants.LOG_DIR_EXPANSION_VAR
						+ "/taskmanager-stderr.log";
				ctx.setCommands(Collections.singletonList(tmCommand));
				
				LOG.info("Starting TM with command="+tmCommand);
				
				// copy resources to the TaskManagers.
				Map<String, LocalResource> localResources = new HashMap<String, LocalResource>(2);
				localResources.put("stratosphere.jar", stratosphereJar);
				localResources.put("stratosphere-conf.yaml", stratosphereConf);
				
				// add ship resources
				if(!shipListString.isEmpty()) {
					Preconditions.checkNotNull(remoteShipRsc);
					for( int i = 0; i < remoteShipPaths.length; i++) {
						localResources.put(new Path(remoteShipPaths[i]).getName(), remoteShipRsc[i]);
					}
				}
				
				
				ctx.setLocalResources(localResources);
				
				// Setup CLASSPATH for Container (=TaskTracker)
				Map<String, String> containerEnv = new HashMap<String, String>();
				Utils.setupEnv(conf, containerEnv); //add stratosphere.jar to class path.
				containerEnv.put(Client.ENV_CLIENT_USERNAME, yarnClientUsername);
				
				ctx.setEnvironment(containerEnv);

				UserGroupInformation user = UserGroupInformation.getCurrentUser();
				try {
					Credentials credentials = user.getCredentials();
					DataOutputBuffer dob = new DataOutputBuffer();
					credentials.writeTokenStorageToStream(dob);
					ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(),
							0, dob.getLength());
					ctx.setTokens(securityTokens);
				} catch (IOException e) {
					LOG.warn("Getting current user info failed when trying to launch the container"
							+ e.getMessage());
				}
				
				LOG.info("Launching container " + allocatedContainers);
				nmClient.startContainer(container, ctx);
			}
			for (ContainerStatus status : response.getCompletedContainersStatuses()) {
				++completedContainers;
				LOG.info("Completed container (while allocating) "+status.getContainerId()+". Total Completed:" + completedContainers);
				LOG.info("Diagnostics "+status.getDiagnostics());
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
				LOG.info("Diagnostics "+status.getDiagnostics());
			}
			Thread.sleep(5000);
		}
		LOG.info("Shutting down JobManager");
		jmr.shutdown();
		jmr.join(500);
		
		// Un-register with ResourceManager
		rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
		
		
	}
	public static void main(String[] args) throws Exception {
		final String yarnClientUsername = System.getenv(Client.ENV_CLIENT_USERNAME);
		LOG.info("YARN daemon runs as '"+UserGroupInformation.getCurrentUser().getShortUserName()+"' setting"
				+ " user to execute Stratosphere ApplicationMaster/JobManager to '"+yarnClientUsername+"'");
		UserGroupInformation ugi = UserGroupInformation.createRemoteUser(yarnClientUsername);
		for(Token<? extends TokenIdentifier> toks : UserGroupInformation.getCurrentUser().getTokens()) {
			ugi.addToken(toks);
		}
		ugi.doAs(new PrivilegedAction<Object>() {
			@Override
			public Object run() {
				try {
					new ApplicationMaster().run();
				} catch (Exception e) {
					e.printStackTrace();
				}
				return null;
			}
		});
	}
}
