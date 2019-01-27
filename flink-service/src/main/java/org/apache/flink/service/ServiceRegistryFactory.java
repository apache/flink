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

package org.apache.flink.service;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for accessing Registry.
 */
public final class ServiceRegistryFactory {

	private static final Logger logger = LoggerFactory.getLogger(ServiceRegistryFactory.class);

	private static final String SERVICE_REGISTRY_CLASS = "flink.service.registry.class";

	private static final String DEFAULT_REGISTRY_CLASS = "org.apache.flink.service.impl.FileSystemRegistry";

	private ServiceRegistryFactory() {}

	public static ServiceRegistry getRegistry() {
		Configuration configuration = GlobalConfiguration.loadConfiguration();
		try {
			String className = configuration.getString(SERVICE_REGISTRY_CLASS, DEFAULT_REGISTRY_CLASS);
			return (ServiceRegistry) Class.forName(className).newInstance();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new RuntimeException(e);
		}
	}

}
