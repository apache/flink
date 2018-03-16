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

package org.apache.flink.storm.api;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.program.ContextEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.util.Preconditions;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.SubmitOptions;
import org.apache.storm.utils.Utils;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;

/**
 * {@link FlinkSubmitter} mimics a {@link StormSubmitter} to submit Storm topologies to a Flink cluster.
 */
public class FlinkSubmitter {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkSubmitter.class);

	/**
	 * Submits a topology to run on the cluster. A topology runs forever or until explicitly killed.
	 *
	 * @param name
	 * 		the name of the storm.
	 * @param stormConf
	 * 		the topology-specific configuration. See {@link Config}.
	 * @param topology
	 * 		the processing to execute.
	 * @param opts
	 * 		to manipulate the starting of the topology.
	 * @throws AlreadyAliveException
	 * 		if a topology with this name is already running
	 * @throws InvalidTopologyException
	 * 		if an invalid topology was submitted
	 */
	public static void submitTopology(final String name, final Map<?, ?> stormConf, final FlinkTopology topology,
			final SubmitOptions opts)
					throws AlreadyAliveException, InvalidTopologyException {
		submitTopology(name, stormConf, topology);
	}

	/**
	 * Submits a topology to run on the cluster. A topology runs forever or until explicitly killed. The given {@link
	 * FlinkProgressListener} is ignored because progress bars are not supported by Flink.
	 *
	 * @param name
	 * 		the name of the storm.
	 * @param stormConf
	 * 		the topology-specific configuration. See {@link Config}.
	 * @param topology
	 * 		the processing to execute.
	 * @throws AlreadyAliveException
	 * 		if a topology with this name is already running
	 * @throws InvalidTopologyException
	 * 		if an invalid topology was submitted
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	public static void submitTopology(final String name, final Map stormConf, final FlinkTopology topology)
			throws AlreadyAliveException, InvalidTopologyException {
		if (!Utils.isValidConf(stormConf)) {
			throw new IllegalArgumentException("Storm conf is not valid. Must be json-serializable");
		}

		final Configuration flinkConfig = GlobalConfiguration.loadConfiguration();
		if (!stormConf.containsKey(Config.NIMBUS_HOST)) {
			stormConf.put(Config.NIMBUS_HOST,
					flinkConfig.getString(JobManagerOptions.ADDRESS, "localhost"));
		}
		if (!stormConf.containsKey(Config.NIMBUS_THRIFT_PORT)) {
			stormConf.put(Config.NIMBUS_THRIFT_PORT,
					new Integer(flinkConfig.getInteger(JobManagerOptions.PORT)));
		}

		final String serConf = JSONValue.toJSONString(stormConf);

		final FlinkClient client = FlinkClient.getConfiguredClient(stormConf);

		try {
			if (client.getTopologyJobId(name) != null) {
				throw new RuntimeException("Topology with name `" + name + "` already exists on cluster");
			}
			String localJar = System.getProperty("storm.jar");
			if (localJar == null) {
				try {
					for (final URL url : ((ContextEnvironment) ExecutionEnvironment.getExecutionEnvironment())
							.getJars()) {
						// TODO verify that there is only one jar
						localJar = new File(url.toURI()).getAbsolutePath();
					}
				} catch (final URISyntaxException e) {
					// ignore
				} catch (final ClassCastException e) {
					// ignore
				}
			}
			Preconditions.checkNotNull(localJar, "LocalJar must not be null.");
			LOG.info("Submitting topology " + name + " in distributed mode with conf " + serConf);
			client.submitTopologyWithOpts(name, localJar, topology);
		} catch (final InvalidTopologyException e) {
			LOG.warn("Topology submission exception: " + e.get_msg());
			throw e;
		} catch (final AlreadyAliveException e) {
			LOG.warn("Topology already alive exception", e);
			throw e;
		}

		LOG.info("Finished submitting topology: " + name);
	}

	/**
	 * Same as {@link #submitTopology(String, Map, FlinkTopology, SubmitOptions)}. Progress bars are not supported by
	 * Flink.
	 *
	 * @param name
	 * 		the name of the storm.
	 * @param stormConf
	 * 		the topology-specific configuration. See {@link Config}.
	 * @param topology
	 * 		the processing to execute.
	 * @throws AlreadyAliveException
	 * 		if a topology with this name is already running
	 * @throws InvalidTopologyException
	 * 		if an invalid topology was submitted
	 */
	public static void submitTopologyWithProgressBar(final String name, final Map<?, ?> stormConf,
			final FlinkTopology topology)
					throws AlreadyAliveException, InvalidTopologyException {
		submitTopology(name, stormConf, topology);
	}

	/**
	 * In Flink, jar files are submitted directly when a program is started. Thus, this method does nothing. The
	 * returned value is parameter localJar, because this give the best integration of Storm behavior within a Flink
	 * environment.
	 *
	 * @param conf
	 * 		the topology-specific configuration. See {@link Config}.
	 * @param localJar
	 * 		file path of the jar file to submit
	 * @return the value of parameter localJar
	 */
	@SuppressWarnings("rawtypes")
	public static String submitJar(final Map conf, final String localJar) {
		return submitJar(localJar);
	}

	/**
	 * In Flink, jar files are submitted directly when a program is started. Thus, this method does nothing. The
	 * returned value is parameter localJar, because this give the best integration of Storm behavior within a Flink
	 * environment.
	 *
	 * @param localJar
	 * 		file path of the jar file to submit
	 * @return the value of parameter localJar
	 */
	public static String submitJar(final String localJar) {
		if (localJar == null) {
			throw new RuntimeException(
					"Must submit topologies using the 'storm' client script so that StormSubmitter knows which jar " +
					"to upload");
		}

		return localJar;
	}

	/**
	 * Dummy interface use to track progress of file upload. Does not do anything. Kept for compatibility.
	 */
	public interface FlinkProgressListener {
	}

}
