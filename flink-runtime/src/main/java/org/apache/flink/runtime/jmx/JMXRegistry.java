/*
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

package org.apache.flink.runtime.jmx;

import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceListener;
import org.apache.flink.runtime.instance.InstanceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

/**
 * Registers/deregisters task manager instances with the MBean server. The instances implement the
 * {@link InstanceReporterMXBean} interface which the MBeanServer uses to extract data.
 */
public class JMXRegistry implements InstanceListener {

	private final static Logger LOG = LoggerFactory.getLogger(JMXRegistry.class);

	private final static MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

	private static JMXRegistry instance;

	private Map<Instance, ObjectName> registeredBeans = new HashMap<>();

	private JMXRegistry (InstanceManager manager){
		manager.addInstanceListener(this);
	}

	@Override
	public void newInstanceAvailable(Instance taskManager) {
		try {
			ObjectName name = new ObjectName("org.apache.flink:type=TaskManager,id=" + taskManager.getId());
			registeredBeans.put(taskManager, name);
			mbs.registerMBean(taskManager, name);
		} catch (Exception e) {
			LOG.error("Couldn't expose task manager with id {}", taskManager.getId(), e);
		}
	}

	@Override
	public void instanceDied(Instance instance) {
		ObjectName objectName = registeredBeans.get(instance);
		try {
			mbs.unregisterMBean(objectName);
		} catch (Exception e) {
			LOG.error("Couldn't unregister task manager with id {}", instance.getId(), e);
		}
	}

	public static void setup(InstanceManager manager) {
		if (instance == null) {
			instance = new JMXRegistry(manager);
		} else {
			throw new IllegalStateException("The JMXRegistry should only be initialized once per VM.");
		}
	}

}
