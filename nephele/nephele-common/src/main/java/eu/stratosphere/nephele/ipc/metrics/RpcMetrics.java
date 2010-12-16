/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.ipc.metrics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import eu.stratosphere.nephele.ipc.Server;
import eu.stratosphere.nephele.metrics.MetricsContext;
import eu.stratosphere.nephele.metrics.MetricsRecord;
import eu.stratosphere.nephele.metrics.MetricsUtil;
import eu.stratosphere.nephele.metrics.Updater;
import eu.stratosphere.nephele.metrics.util.MetricsBase;
import eu.stratosphere.nephele.metrics.util.MetricsIntValue;
import eu.stratosphere.nephele.metrics.util.MetricsRegistry;
import eu.stratosphere.nephele.metrics.util.MetricsTimeVaryingRate;

/**
 * This class is for maintaining the various RPC statistics
 * and publishing them through the metrics interfaces.
 * This also registers the JMX MBean for RPC.
 * <p>
 * This class has a number of metrics variables that are publicly accessible; these variables (objects) have methods to
 * update their values; for example:
 * <p>
 * {@link #rpcQueueTime}.inc(time)
 */
public class RpcMetrics implements Updater {
	public MetricsRegistry registry = new MetricsRegistry();

	private MetricsRecord metricsRecord;

	private Server myServer;

	private static Log LOG = LogFactory.getLog(RpcMetrics.class);

	RpcActivityMBean rpcMBean;

	public RpcMetrics(String hostName, String port, Server server) {
		myServer = server;
		MetricsContext context = MetricsUtil.getContext("rpc");
		metricsRecord = MetricsUtil.createRecord(context, "metrics");

		metricsRecord.setTag("port", port);

		LOG.info("Initializing RPC Metrics with hostName=" + hostName + ", port=" + port);

		context.registerUpdater(this);

		// Need to clean up the interface to RpcMgt - don't need both metrics and server params
		rpcMBean = new RpcActivityMBean(registry, hostName, port);
	}

	/**
	 * The metrics variables are public:
	 * - they can be set directly by calling their set/inc methods
	 * -they can also be read directly - e.g. JMX does this.
	 */

	public MetricsTimeVaryingRate rpcQueueTime = new MetricsTimeVaryingRate("RpcQueueTime", registry);

	public MetricsTimeVaryingRate rpcProcessingTime = new MetricsTimeVaryingRate("RpcProcessingTime", registry);

	public MetricsIntValue numOpenConnections = new MetricsIntValue("NumOpenConnections", registry);

	public MetricsIntValue callQueueLen = new MetricsIntValue("callQueueLen", registry);

	/**
	 * Push the metrics to the monitoring subsystem on doUpdate() call.
	 */
	public void doUpdates(MetricsContext context) {

		synchronized (this) {
			// ToFix - fix server to use the following two metrics directly so
			// the metrics do not have be copied here.
			numOpenConnections.set(myServer.getNumOpenConnections());
			callQueueLen.set(myServer.getCallQueueLen());
			for (MetricsBase m : registry.getMetricsList()) {
				m.pushMetric(metricsRecord);
			}
		}
		metricsRecord.update();
	}

	public void shutdown() {
		if (rpcMBean != null)
			rpcMBean.shutdown();
	}
}
