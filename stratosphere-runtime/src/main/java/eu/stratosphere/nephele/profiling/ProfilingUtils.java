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

package eu.stratosphere.nephele.profiling;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * This class contains utility functions to load and configure Nephele's
 * profiling component.
 * 
 * @author warneke
 */
public class ProfilingUtils {

	/**
	 * The logging instance used to report problems.
	 */
	private static final Log LOG = LogFactory.getLog(ProfilingUtils.class);

	/**
	 * The key to check the job manager's profiling component should be enabled.
	 */
	public static final String ENABLE_PROFILING_KEY = "jobmanager.profiling.enable";

	/**
	 * The class name of the the job manager's profiling component to load if progiling is enabled.
	 */
	public static final String JOBMANAGER_CLASSNAME_KEY = "jobmanager.profiling.classname";

	/**
	 * The class name of the task manager's profiling component to load if profiling is enabled.
	 */
	public static final String TASKMANAGER_CLASSNAME_KEY = "taskmanager.profiling.classname";

	/**
	 * The key to check whether job profiling should be enabled for a specific job.
	 */
	public static final String PROFILE_JOB_KEY = "job.profiling.enable";

	/**
	 * The key to check the port of the job manager's profiling RPC server.
	 */
	public static final String JOBMANAGER_RPC_PORT_KEY = "jobmanager.profiling.rpc.port";

	/**
	 * The default network port the job manager's profiling component starts its RPC server on.
	 */
	public static final int JOBMANAGER_DEFAULT_RPC_PORT = 6124;

	/**
	 * Key to interval in which a task manager is supposed to send profiling data to the job manager.
	 */
	public static final String TASKMANAGER_REPORTINTERVAL_KEY = "taskmanager.profiling.reportinterval";

	/**
	 * Default interval which the task manager uses to report profiling data to the job manager.
	 */
	public static final int DEFAULT_TASKMANAGER_REPORTINTERVAL = 2;

	/**
	 * Creates an instance of the job manager's profiling component.
	 * 
	 * @param profilerClassName
	 *        the class name of the profiling component to load
	 * @param jobManagerBindAddress
	 *        the address the job manager's RPC server is bound to
	 * @return an instance of the job manager profiling component or <code>null</code> if an error occurs
	 */
	@SuppressWarnings("unchecked")
	public static JobManagerProfiler loadJobManagerProfiler(String profilerClassName, InetAddress jobManagerBindAddress) {

		final Class<? extends JobManagerProfiler> profilerClass;
		try {
			profilerClass = (Class<? extends JobManagerProfiler>) Class.forName(profilerClassName);
		} catch (ClassNotFoundException e) {
			LOG.error("Cannot find class " + profilerClassName + ": " + StringUtils.stringifyException(e));
			return null;
		}

		JobManagerProfiler profiler = null;
		
		try {
			
			final Constructor<JobManagerProfiler> constr = (Constructor<JobManagerProfiler>) profilerClass.getConstructor(InetAddress.class);
			profiler = constr.newInstance(jobManagerBindAddress);
		
		} catch(InvocationTargetException e) {
			LOG.error("Cannot create profiler: " + StringUtils.stringifyException(e));
			return null;
		} catch (NoSuchMethodException e) {
			LOG.error("Cannot create profiler: " + StringUtils.stringifyException(e));
			return null;
		} catch (InstantiationException e) {
			LOG.error("Cannot create profiler: " + StringUtils.stringifyException(e));
			return null;
		} catch (IllegalAccessException e) {
			LOG.error("Cannot create profiler: " + StringUtils.stringifyException(e));
			return null;
		} catch (IllegalArgumentException e) {
			LOG.error("Cannot create profiler: " + StringUtils.stringifyException(e));
			return null;
		}

		return profiler;
	}

	/**
	 * Creates an instance of the task manager's profiling component.
	 * 
	 * @param profilerClassName
	 *        the class name of the profiling component to load
	 * @return an instance of the task manager profiling component or <code>null</code> if an error occurs
	 */
	@SuppressWarnings("unchecked")
	public static TaskManagerProfiler loadTaskManagerProfiler(String profilerClassName, InetAddress jobManagerAddress,
			InstanceConnectionInfo instanceConnectionInfo) {

		final Class<? extends TaskManagerProfiler> profilerClass;
		try {
			profilerClass = (Class<? extends TaskManagerProfiler>) Class.forName(profilerClassName);
		} catch (ClassNotFoundException e) {
			LOG.error("Cannot find class " + profilerClassName + ": " + StringUtils.stringifyException(e));
			return null;
		}

		Constructor<? extends TaskManagerProfiler> constructor = null;
		try {
			constructor = profilerClass.getConstructor(InetAddress.class, InstanceConnectionInfo.class);
		} catch (SecurityException e1) {
			LOG.error(e1);
			return null;
		} catch (NoSuchMethodException e1) {
			LOG.error(e1);
			return null;
		}

		TaskManagerProfiler profiler = null;
		try {
			profiler = constructor.newInstance(jobManagerAddress, instanceConnectionInfo);
		} catch (IllegalArgumentException e) {
			LOG.error(e);
		} catch (InstantiationException e) {
			LOG.error(e);
		} catch (IllegalAccessException e) {
			LOG.error(e);
		} catch (InvocationTargetException e) {
			LOG.error(e);
		}

		return profiler;
	}
}
