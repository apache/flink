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


package org.apache.flink.runtime.jobmanager;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.runtime.ExecutionMode;
import org.apache.flink.runtime.instance.InstanceManager;
import org.apache.flink.runtime.jobmanager.scheduler.DefaultScheduler;
import org.apache.flink.util.StringUtils;

/**
 * This class provides static utility methods for the {@link JobManager}.
 * 
 */
public class JobManagerUtils {

	/**
	 * The logging object used by the utility methods.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(JobManagerUtils.class);

	/**
	 * Private constructor.
	 */
	private JobManagerUtils() {
	}

	/**
	 * Tries to locate a class with given name and to
	 * instantiate a {@link org.apache.flink.runtime.jobmanager.scheduler.DefaultScheduler} object from it.
	 * 
	 * @param schedulerClassName
	 *        the name of the class to instantiate the scheduler object from
	 * @param deploymentManager
	 *        the deployment manager which shall be passed on to the scheduler
	 * @param instanceManager
	 *        the instance manager which shall be passed on to the scheduler
	 * @return the {@link org.apache.flink.runtime.jobmanager.scheduler.DefaultScheduler} object instantiated from the class with the provided name
	 */
	@SuppressWarnings("unchecked")
	static DefaultScheduler loadScheduler(final String schedulerClassName, final DeploymentManager deploymentManager,
			final InstanceManager instanceManager) {

		Class<? extends DefaultScheduler> schedulerClass;
		try {
			schedulerClass = (Class<? extends DefaultScheduler>) Class.forName(schedulerClassName);
		} catch (ClassNotFoundException e) {
			LOG.error("Cannot find class " + schedulerClassName + ": " + StringUtils.stringifyException(e));
			return null;
		}

		Constructor<? extends DefaultScheduler> constructor;

		try {

			Class<?>[] constructorArgs = { DeploymentManager.class, InstanceManager.class };
			constructor = schedulerClass.getConstructor(constructorArgs);
		} catch (NoSuchMethodException e) {
			LOG.error("Cannot create scheduler: " + StringUtils.stringifyException(e));
			return null;
		} catch (SecurityException e) {
			LOG.error("Cannot create scheduler: " + StringUtils.stringifyException(e));
			return null;
		}

		DefaultScheduler scheduler;

		try {
			scheduler = constructor.newInstance(deploymentManager, instanceManager);
		} catch (InstantiationException e) {
			LOG.error("Cannot create scheduler: " + StringUtils.stringifyException(e));
			return null;
		} catch (IllegalAccessException e) {
			LOG.error("Cannot create scheduler: " + StringUtils.stringifyException(e));
			return null;
		} catch (IllegalArgumentException e) {
			LOG.error("Cannot create scheduler: " + StringUtils.stringifyException(e));
			return null;
		} catch (InvocationTargetException e) {
			LOG.error("Cannot create scheduler: " + StringUtils.stringifyException(e));
			return null;
		}

		return scheduler;
	}

	/**
	 * Tries to read the class name of the {@link org.apache.flink.runtime.jobmanager.scheduler.DefaultScheduler} implementation from the global configuration which
	 * is set to be used for the provided execution mode.
	 * 
	 * @param executionMode The Nephele execution mode.
	 * @return the class name of the {@link org.apache.flink.runtime.jobmanager.scheduler.DefaultScheduler} implementation to be used or <code>null</code> if no
	 *         implementation is configured for the given execution mode
	 */
	static String getSchedulerClassName(ExecutionMode executionMode) {
		return "org.apache.flink.runtime.jobmanager.scheduler.DefaultScheduler";
	}
}
