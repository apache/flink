/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.stormcompatibility.api;

import java.io.File;
import java.util.Map;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.program.ContextEnvironment;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.SubmitOptions;
import backtype.storm.utils.Utils;





/**
 * {@link FlinkSubmitter} mimics a {@link StormSubmitter} to submit Storm topologies to a Flink cluster.
 */
public class FlinkSubmitter {
	public static Logger logger = LoggerFactory.getLogger(FlinkSubmitter.class);
	
	/**
	 * Submits a topology to run on the cluster. A topology runs forever or until explicitly killed.
	 * 
	 * 
	 * @param name
	 *            the name of the storm.
	 * @param stormConf
	 *            the topology-specific configuration. See {@link Config}.
	 * @param topology
	 *            the processing to execute.
	 * @throws AlreadyAliveException
	 *             if a topology with this name is already running
	 * @throws InvalidTopologyException
	 *             if an invalid topology was submitted
	 */
	public static void submitTopology(final String name, final Map<?, ?> stormConf, final FlinkTopology topology)
		throws AlreadyAliveException, InvalidTopologyException {
		submitTopology(name, stormConf, topology, (SubmitOptions)null, (FlinkProgressListener)null);
	}
	
	/**
	 * Submits a topology to run on the cluster. A topology runs forever or until explicitly killed.
	 * 
	 * @param name
	 *            the name of the storm.
	 * @param stormConf
	 *            the topology-specific configuration. See {@link Config}.
	 * @param topology
	 *            the processing to execute.
	 * @param opts
	 *            to manipulate the starting of the topology.
	 * @throws AlreadyAliveException
	 *             if a topology with this name is already running
	 * @throws InvalidTopologyException
	 *             if an invalid topology was submitted
	 */
	public static void submitTopology(final String name, final Map<?, ?> stormConf, final FlinkTopology topology, final SubmitOptions opts)
		throws AlreadyAliveException, InvalidTopologyException {
		submitTopology(name, stormConf, topology, opts, (FlinkProgressListener)null);
	}
	
	/**
	 * Submits a topology to run on the cluster. A topology runs forever or until explicitly killed. The given
	 * {@link FlinkProgressListener} is ignored because progress bars are not supported by Flink.
	 * 
	 * 
	 * @param name
	 *            the name of the storm.
	 * @param stormConf
	 *            the topology-specific configuration. See {@link Config}.
	 * @param topology
	 *            the processing to execute.
	 * @param opts
	 *            to manipulate the starting of the topology
	 * @param progressListener
	 *            to track the progress of the jar upload process
	 * @throws AlreadyAliveException
	 *             if a topology with this name is already running
	 * @throws InvalidTopologyException
	 *             if an invalid topology was submitted
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public static void submitTopology(final String name, final Map stormConf, final FlinkTopology topology, final SubmitOptions opts, final FlinkProgressListener progressListener)
		throws AlreadyAliveException, InvalidTopologyException {
		if(!Utils.isValidConf(stormConf)) {
			throw new IllegalArgumentException("Storm conf is not valid. Must be json-serializable");
		}
		
		final Configuration flinkConfig = GlobalConfiguration.getConfiguration();
		if(!stormConf.containsKey(Config.NIMBUS_HOST)) {
			stormConf.put(Config.NIMBUS_HOST,
				flinkConfig.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost"));
		}
		if(!stormConf.containsKey(Config.NIMBUS_THRIFT_PORT)) {
			stormConf.put(Config.NIMBUS_THRIFT_PORT,
				new Integer(flinkConfig.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, 6123)));
		}
		
		final String serConf = JSONValue.toJSONString(stormConf);
		
		final FlinkClient client = FlinkClient.getConfiguredClient(stormConf);
		if(client.getTopologyJobId(name) != null) {
			throw new RuntimeException("Topology with name `" + name + "` already exists on cluster");
		}
		String localJar = System.getProperty("storm.jar");
		if(localJar == null) {
			try {
				for(final File file : ((ContextEnvironment)ExecutionEnvironment.getExecutionEnvironment()).getJars()) {
					// should only be one jar file...
					// TODO verify above assumption
					localJar = file.getAbsolutePath();
				}
			} catch(final ClassCastException e) {
				// ignore
			}
		}
		try {
			logger.info("Submitting topology " + name + " in distributed mode with conf " + serConf);
			client.submitTopologyWithOpts(name, localJar, serConf, topology, opts);
		} catch(final InvalidTopologyException e) {
			logger.warn("Topology submission exception: " + e.get_msg());
			throw e;
		} catch(final AlreadyAliveException e) {
			logger.warn("Topology already alive exception", e);
			throw e;
		} finally {
			client.close();
		}
		
		logger.info("Finished submitting topology: " + name);
	}
	
	/**
	 * Same as {@link #submitTopology(String, Map, FlinkTopology)}. Progress bars are not supported by Flink.
	 * 
	 * @param name
	 *            the name of the storm.
	 * @param stormConf
	 *            the topology-specific configuration. See {@link Config}.
	 * @param topology
	 *            the processing to execute.
	 * @throws AlreadyAliveException
	 *             if a topology with this name is already running
	 * @throws InvalidTopologyException
	 *             if an invalid topology was submitted
	 */
	
	public static void submitTopologyWithProgressBar(final String name, final Map<?, ?> stormConf, final FlinkTopology topology)
		throws AlreadyAliveException, InvalidTopologyException {
		submitTopologyWithProgressBar(name, stormConf, topology, null);
	}
	
	/**
	 * Same as {@link #submitTopology(String, Map, FlinkTopology, SubmitOptions)}. Progress bars are not supported by
	 * Flink.
	 * 
	 * @param name
	 *            the name of the storm.
	 * @param stormConf
	 *            the topology-specific configuration. See {@link Config}.
	 * @param topology
	 *            the processing to execute.
	 * @param opts
	 *            to manipulate the starting of the topology
	 * @throws AlreadyAliveException
	 *             if a topology with this name is already running
	 * @throws InvalidTopologyException
	 *             if an invalid topology was submitted
	 */
	
	public static void submitTopologyWithProgressBar(final String name, final Map<?, ?> stormConf, final FlinkTopology topology, final SubmitOptions opts)
		throws AlreadyAliveException, InvalidTopologyException {
		submitTopology(name, stormConf, topology, opts, null);
	}
	
	/**
	 * In Flink, jar files are submitted directly when a program is started. Thus, this method does nothing. The
	 * returned value is parameter localJar, because this give the best integration of Storm behavior within a Flink
	 * environment.
	 * 
	 * @param conf
	 *            the topology-specific configuration. See {@link Config}.
	 * @param localJar
	 *            file path of the jar file to submit
	 * @return the value of parameter localJar
	 */
	public static String submitJar(@SuppressWarnings("rawtypes") final Map conf, final String localJar) {
		return submitJar(conf, localJar, (FlinkProgressListener)null);
	}
	
	/**
	 * In Flink, jar files are submitted directly when a program is started. Thus, this method does nothing. The
	 * returned value is parameter localJar, because this give the best integration of Storm behavior within a Flink
	 * environment.
	 * 
	 * @param conf
	 *            the topology-specific configuration. See {@link Config}.
	 * @param localJar
	 *            file path of the jar file to submit
	 * @param listener
	 *            progress listener to track the jar file upload
	 * @return the value of parameter localJar
	 */
	public static String submitJar(@SuppressWarnings("rawtypes") final Map conf, final String localJar, final FlinkProgressListener listener) {
		if(localJar == null) {
			throw new RuntimeException(
				"Must submit topologies using the 'storm' client script so that StormSubmitter knows which jar to upload.");
		}
		
		return localJar;
	}
	
	/**
	 * Dummy interface use to track progress of file upload. Does not do anything. Kept for compatibility.
	 */
	public interface FlinkProgressListener { /* empty */}
	
}
