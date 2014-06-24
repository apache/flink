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

package eu.stratosphere.nephele.jobmanager;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

import eu.stratosphere.nephele.ExecutionMode;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.jobmanager.scheduler.DefaultScheduler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.util.StringUtils;

/**
 * This class provides static utility methods for the {@link JobManager}.
 * 
 */
public class JobManagerUtils {

	/**
	 * The logging object used by the utility methods.
	 */
	private static final Log LOG = LogFactory.getLog(JobManagerUtils.class);

	/**
	 * Private constructor.
	 */
	private JobManagerUtils() {
	}

	/**
	 * Tries to locate a class with given name and to
	 * instantiate a {@link eu.stratosphere.nephele.jobmanager.scheduler.DefaultScheduler} object from it.
	 * 
	 * @param schedulerClassName
	 *        the name of the class to instantiate the scheduler object from
	 * @param deploymentManager
	 *        the deployment manager which shall be passed on to the scheduler
	 * @param instanceManager
	 *        the instance manager which shall be passed on to the scheduler
	 * @return the {@link eu.stratosphere.nephele.jobmanager.scheduler.DefaultScheduler} object instantiated from the class with the provided name
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
	 * Tries to locate a class with given name and to
	 * instantiate a instance manager from it.
	 * 
	 * @param instanceManagerClassName
	 *        the name of the class to instantiate the instance manager object from
	 * @return the {@link eu.stratosphere.nephele.instance.InstanceManager} object instantiated from the class with the provided name
	 */
	@SuppressWarnings("unchecked")
	static InstanceManager loadInstanceManager(final String instanceManagerClassName) {

		Class<? extends InstanceManager> instanceManagerClass;
		try {
			instanceManagerClass = (Class<? extends InstanceManager>) Class.forName(instanceManagerClassName);
		} catch (ClassNotFoundException e) {
			LOG.error("Cannot find class " + instanceManagerClassName + ": " + StringUtils.stringifyException(e));
			return null;
		}

		InstanceManager instanceManager;

		try {
			instanceManager = instanceManagerClass.newInstance();
		} catch (InstantiationException e) {
			LOG.error("Cannot create instanceManager: " + StringUtils.stringifyException(e));
			return null;
		} catch (IllegalAccessException e) {
			LOG.error("Cannot create instanceManager: " + StringUtils.stringifyException(e));
			return null;
		}

		return instanceManager;
	}

	/**
	 * Tries to read the class name of the {@link eu.stratosphere.nephele.jobmanager.scheduler.DefaultScheduler} implementation from the global configuration which
	 * is set to be used for the provided execution mode.
	 * 
	 * @param executionMode The Nephele execution mode.
	 * @return the class name of the {@link eu.stratosphere.nephele.jobmanager.scheduler.DefaultScheduler} implementation to be used or <code>null</code> if no
	 *         implementation is configured for the given execution mode
	 */
	static String getSchedulerClassName(ExecutionMode executionMode) {
		return "eu.stratosphere.nephele.jobmanager.scheduler.DefaultScheduler";
	}

	/**
	 * Tries to read the class name of the {@link eu.stratosphere.nephele.instance.InstanceManager} implementation from the global configuration which is
	 * set to be used for the provided execution mode.
	 * 
	 * @param executionMode The Nephele execution mode.
	 * @return the class name of the {@link eu.stratosphere.nephele.instance.InstanceManager} implementation to be used or <code>null</code> if no
	 *         implementation is configured for the given execution mode
	 */
	static String getInstanceManagerClassName(ExecutionMode executionMode) {
		switch (executionMode) {
		case LOCAL:
			return "eu.stratosphere.nephele.instance.LocalInstanceManager";
		case CLUSTER:
			return "eu.stratosphere.nephele.instance.DefaultInstanceManager";
		default:
			throw new RuntimeException("Unrecognized Execution Mode.");
		}
	}
	
	/**
	 * Returns the version of Stratosphere as String.
	 * If version == null, then the JobManager runs from inside the IDE (or somehow not from the maven build jar)
	 * @return String
	 */
	public static String getVersion() {
		String version = JobManagerUtils.class.getPackage().getImplementationVersion();
		return version;
	}

	/**
	 * Returns the revision of Stratosphere as String.
	 * @return String
	 */
	public static RevisionInformation getRevisionInformation() {
		RevisionInformation info = new RevisionInformation();
		String revision = "<unknown>";
		String commitDate = "<unknown>";
		try {
			Properties properties = new Properties();
			InputStream propFile = JobManagerUtils.class.getClassLoader().getResourceAsStream(".version.properties");
			if (propFile != null) {
				properties.load(propFile);
				revision = properties.getProperty("git.commit.id.abbrev");
				commitDate = properties.getProperty("git.commit.time");
			}
		} catch (IOException e) {
			LOG.info("Cannot determine code revision. Unable ro read version property file.");
		}
		info.commitId = revision;
		info.commitDate = commitDate;
		return info;
	}
	
	public static class RevisionInformation {
		public String commitId;
		public String commitDate;
	}
}
