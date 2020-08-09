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

package org.apache.flink.python.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * A Util class to get the {@link StreamExecutionEnvironment} configuration and merged configuration with environment
 * settings.
 */
public class PythonConfigUtil {

	/**
	 * A static method to get the {@link StreamExecutionEnvironment} configuration merged with python dependency
	 * management configurations.
	 */
	public static Configuration getMergedConfig(StreamExecutionEnvironment env) throws InvocationTargetException,
		IllegalAccessException, NoSuchMethodException {
		Configuration envConfiguration = getEnvironmentConfig(env);
		Configuration config = PythonDependencyUtils.configurePythonDependencies(env.getCachedFiles(), envConfiguration);
		return config;
	}

	/**
	 * Get the private method {@link StreamExecutionEnvironment#getConfiguration()} by reflection recursively. Then
	 * access the method to get the configuration of the given StreamExecutionEnvironment.
	 */
	public static Configuration getEnvironmentConfig(StreamExecutionEnvironment env) throws InvocationTargetException,
		IllegalAccessException, NoSuchMethodException {
		Method getConfigurationMethod = null;
		for (Class<?> clz = env.getClass(); clz != Object.class; clz = clz.getSuperclass()) {
			try {
				getConfigurationMethod = clz.getDeclaredMethod("getConfiguration");
				break;
			} catch (NoSuchMethodException e) {

			}
		}

		if (getConfigurationMethod == null) {
			throw new NoSuchMethodException("Method getConfigurationMethod not found.");
		}

		getConfigurationMethod.setAccessible(true);
		Configuration envConfiguration = (Configuration) getConfigurationMethod.invoke(env);
		return envConfiguration;
	}
}
