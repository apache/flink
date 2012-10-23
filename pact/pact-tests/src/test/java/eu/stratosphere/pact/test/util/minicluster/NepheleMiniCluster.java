/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.test.util.minicluster;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
import eu.stratosphere.nephele.instance.local.LocalInstanceManager;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobmanager.JobManager;
import eu.stratosphere.nephele.jobmanager.scheduler.local.LocalScheduler;
import eu.stratosphere.nephele.protocols.ExtendedManagementProtocol;
import eu.stratosphere.nephele.rpc.ManagementTypeUtils;
import eu.stratosphere.nephele.rpc.RPCService;
import eu.stratosphere.pact.test.util.FileWriter;

/**
 * @author Erik Nijkamp
 */
public class NepheleMiniCluster {

	private static final boolean DEFAULT_VISUALIZER_ENABLED = true;

	private static final Log LOG = LogFactory.getLog(NepheleMiniCluster.class);

	private final String nepheleConfigDir;

	private final String hdfsConfigDir;

	private final boolean visualizerEnabled;

	private Thread runner;

	private JobManager jobManager;

	public NepheleMiniCluster(String nepheleConfigDir, String hdfsConfigDir) throws Exception {
		this(nepheleConfigDir, hdfsConfigDir, DEFAULT_VISUALIZER_ENABLED);
	}

	public NepheleMiniCluster(String nepheleConfigDir, String hdfsConfigDir, boolean visualizerEnabled)
										throws Exception {
		this.nepheleConfigDir = nepheleConfigDir;
		this.hdfsConfigDir = hdfsConfigDir;
		this.visualizerEnabled = visualizerEnabled;

		initJobManager();
	}

	// ------------------------------------------------------------------------
	// Public methods
	// ------------------------------------------------------------------------

	public JobClient getJobClient(JobGraph jobGraph) throws Exception
	{
		final Configuration configuration = jobGraph.getJobConfiguration();
		configuration.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost");

		return new JobClient(jobGraph, configuration);
	}

	// ------------------------------------------------------------------------
	// Private methods
	// ------------------------------------------------------------------------

	private void initJobManager() throws Exception
	{
		// config
		final String nepheleConfigDirJob = nepheleConfigDir + "/job";
		final int jobManagerRpcPort = ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT;
		final int rpcPort = 6501, dataPort = 7501;

		new FileWriter()
			.dir(nepheleConfigDirJob)
			.file("nephele-user.xml")
			.write(
					"<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
					"<configuration>",
					"    <property>",
					"        <key>jobmanager.instancemanager.local.classname</key>",
					"        <value>" + LocalInstanceManager.class.getName() + "</value>",
					"    </property>",
					"    <property>",
					"        <key>jobmanager.scheduler.local.classname</key>",
					"        <value>" + LocalScheduler.class.getName() + "</value>",
					"    </property>",
					"    <property>",
					"        <key>fs.hdfs.hdfsdefault</key>",
					"        <value>" + hdfsConfigDir + "/hadoop-default.xml</value>",
					"    </property>",
					"    <property>",
					"        <key>fs.hdfs.hdfssite</key>",
					"        <value>" + hdfsConfigDir + "/hadoop-site.xml</value>",
					"    </property>",
					"    <property>",
					"        <key>" + ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY + "</key>",
					"        <value>localhost</value>",
//					"        <value>" + getLocalIpAddress() + "</value>",
					"    </property>",
					"    <property>",
					"        <key>" + ConfigConstants.JOB_MANAGER_IPC_PORT_KEY + "</key>",
					"        <value>" + jobManagerRpcPort + "</value>",
					"    </property>",
					"    <property>",
					"        <key>" + ConfigConstants.TASK_MANAGER_IPC_PORT_KEY + "</key>",
					"        <value>" + rpcPort + "</value>",
					"    </property>",
					"    <property>",
					"        <key>" + ConfigConstants.TASK_MANAGER_DATA_PORT_KEY + "</key>",
					"        <value>" + dataPort + "</value>",
					"    </property>",
					"    <property>",
					"        <key>" + ConfigConstants.JOB_EXECUTION_RETRIES_KEY + "</key>",
					"        <value>0</value>",
					"    </property>",
					"    <property>",
					"        <key>taskmanager.setup.usediscovery</key>",
					"        <value>false</value>",
					"    </property>",
					"    <property>",
					"        <key>jobmanager.visualization.enable</key>",
					"        <value>" + (visualizerEnabled ? "true" : "false") + "</value>",
					"    </property>",
				"</configuration>")
			.close()
			.file("pact-user.xml")
			.write(
					"<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
					"<configuration>",
//					"    <property>",
//					"        <key>pact.parallelization.degree</key>",
//					"        <value>1</value>",
//					"    </property>",
//					"    <property>",
//					"        <key>pact.parallelization.intra-node-degree</key>",
//					"        <value>1</value>",
//					"    </property>",
//					"    <property>",
//					"        <key>pact.parallelization.maxmachines</key>",
//					"        <value>1</value>",
//					"    </property>",
					"</configuration>")
			.close();

		// thread
		LOG.info("Initializing job manager thread with '" + nepheleConfigDirJob + "'.");
		this.jobManager = new JobManager(nepheleConfigDirJob, "local");
		
		runner = new Thread() {
			@Override
			public void run() {
				// run the main task loop
				jobManager.runTaskLoop();
			}
		};
		this.runner.setDaemon(true);
		this.runner.start();

		waitForJobManagerToBeAvailable("localhost", jobManagerRpcPort, 60 * 1000);
		try {
			Thread.sleep(10000);
		} catch (InterruptedException iex) {}
	}

	public void stop() throws Exception {
		if (jobManager != null) {
			jobManager.shutdown();
		}

		if (runner != null) {
			runner.interrupt();
			runner.join();
			runner = null;
		}
	}

	// ------------------------------------------------------------------------
	// Network utility methods
	// ------------------------------------------------------------------------

	@SuppressWarnings("unused")
	private static String getLocalIpAddress() throws SocketException, Exception {
		String IPv4 = System.getProperty("java.net.preferIPv4Stack");
		return getIPInterfaceAddress("true".equals(IPv4)).getAddress().getHostAddress();
	}

	private static InterfaceAddress getIPInterfaceAddress(boolean preferIPv4) throws Exception, SocketException {
		final List<InterfaceAddress> interfaces = getNetworkInterface().getInterfaceAddresses();
		final Iterator<InterfaceAddress> it = interfaces.iterator();
		final List<InterfaceAddress> matchesIPv4 = new ArrayList<InterfaceAddress>();
		final List<InterfaceAddress> matchesIPv6 = new ArrayList<InterfaceAddress>();

		while (it.hasNext()) {
			final InterfaceAddress ia = it.next();
			if (ia.getBroadcast() != null) {
				if (ia.getAddress() instanceof Inet4Address) {
					matchesIPv4.add(ia);
				} else {
					matchesIPv6.add(ia);
				}
			}
		}

		if (matchesIPv4.isEmpty() && matchesIPv6.isEmpty() == true) {
			throw new Exception("Interface " + getNetworkInterface().getName() + " has no interface address attached.");
		}

		if (preferIPv4 && !matchesIPv4.isEmpty()) {
			for (InterfaceAddress ia : matchesIPv4) {
				if ((ia.getAddress().toString().contains("192") || ia.getAddress().toString().contains("10"))) {
					return ia;
				}
			}
			return matchesIPv4.get(0);
		}

		return !matchesIPv6.isEmpty() ? matchesIPv6.get(0) : matchesIPv4.get(0);
	}

	private static NetworkInterface getNetworkInterface() throws SocketException {
		final Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();

		while (interfaces.hasMoreElements()) {
			NetworkInterface nic = interfaces.nextElement();
			if (!nic.isLoopback() && !nic.isPointToPoint())
				return nic;
		}

		throw new SocketException("Cannot find network interface which is not a loopback interface.");
	}
	
	private static void waitForJobManagerToBeAvailable(String hostname, int port, int timeoutMillies)
	throws IOException
	{
		final InetSocketAddress address = new InetSocketAddress(hostname, port);
		ExtendedManagementProtocol jobManagerConnection = null;
		
		RPCService rpcService = new RPCService(ManagementTypeUtils.getRPCTypesToRegister());
		
		try {
			jobManagerConnection = rpcService.getProxy(address, ExtendedManagementProtocol.class);
			
			Map<InstanceType, InstanceTypeDescription> map = null;
			final long startTime = System.currentTimeMillis();
			
			do {
				try {
					map = jobManagerConnection.getMapOfAvailableInstanceTypes();
					if (map != null && map.size() > 0) {
						break;
					}
				} catch (Exception e) {}
				Thread.sleep(500);
			} while ((System.currentTimeMillis() - startTime) < timeoutMillies);
		}
		catch (Throwable t) {
			LOG.error(t);
			throw new IOException("Waiting for JobManager to come up failed.", t);
		}
		finally {
			
			rpcService.shutDown();
		}
	}
}
