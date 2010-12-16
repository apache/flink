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

import javax.management.ObjectName;

import eu.stratosphere.nephele.metrics.util.MBeanUtil;
import eu.stratosphere.nephele.metrics.util.MetricsDynamicMBeanBase;
import eu.stratosphere.nephele.metrics.util.MetricsRegistry;

/**
 * This is the JMX MBean for reporting the RPC layer Activity.
 * The MBean is register using the name
 * "hadoop:service=<RpcServiceName>,name=RpcActivityForPort<port>"
 * Many of the activity metrics are sampled and averaged on an interval
 * which can be specified in the metrics config file.
 * <p>
 * For the metrics that are sampled and averaged, one must specify a metrics context that does periodic update calls.
 * Most metrics contexts do. The default Null metrics context however does NOT. So if you aren't using any other metrics
 * context then you can turn on the viewing and averaging of sampled metrics by specifying the following two lines in
 * the hadoop-meterics.properties file:
 * 
 * <pre>
 *        rpc.class=org.apache.hadoop.metrics.spi.NullContextWithUpdateThread
 *        rpc.period=10
 * </pre>
 *<p>
 * Note that the metrics are collected regardless of the context used. The context with the update thread is used to
 * average the data periodically Impl details: We use a dynamic mbean that gets the list of the metrics from the metrics
 * registry passed as an argument to the constructor
 */

public class RpcActivityMBean extends MetricsDynamicMBeanBase {
	final private ObjectName mbeanName;

	/**
	 * @param mr
	 *        - the metrics registry that has all the metrics
	 * @param serviceName
	 *        - the service name for the rpc service
	 * @param port
	 *        - the rpc port.
	 */
	public RpcActivityMBean(final MetricsRegistry mr, final String serviceName, final String port) {

		super(mr, "Rpc layer statistics");
		mbeanName = MBeanUtil.registerMBean(serviceName, "RpcActivityForPort" + port, this);
	}

	public void shutdown() {
		if (mbeanName != null)
			MBeanUtil.unregisterMBean(mbeanName);
	}

}
