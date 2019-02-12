/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.yarn.util;

import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.factories.SecurityModuleFactory;
import org.apache.flink.runtime.security.modules.HadoopModule;
import org.apache.flink.runtime.security.modules.SecurityModule;

import org.apache.hadoop.conf.Configuration;

/**
 * Test hadoop security module factory that loads customized hadoop configuration properties.
 */
public class TestHadoopModuleFactory implements SecurityModuleFactory {

	public static final String HADOOP_PROPERTY_CONFIG_KEY = "testing.hadoop.configuration";

	@Override
	public SecurityModule createModule(SecurityConfiguration securityConfig) {
		return new HadoopModule(securityConfig, (Configuration) securityConfig.getProperty(HADOOP_PROPERTY_CONFIG_KEY));
	}
}
