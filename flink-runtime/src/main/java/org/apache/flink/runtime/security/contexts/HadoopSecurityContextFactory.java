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

package org.apache.flink.runtime.security.contexts;

import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityContextInitializeException;
import org.apache.flink.runtime.security.modules.HadoopModuleFactory;

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default security context factory that instantiates {@link SecurityContext}
 * based on installed modules, it would instantiate {@link HadoopSecurityContext} if
 * a {@link HadoopModuleFactory} is included.
 */
public class HadoopSecurityContextFactory implements SecurityContextFactory {
	private static final Logger LOG = LoggerFactory.getLogger(HadoopSecurityContextFactory.class);

	@Override
	public boolean isCompatibleWith(SecurityConfiguration securityConfig) {
		// not compatible if no hadoop module factory configured.
		if (!securityConfig.getSecurityModuleFactories().contains(HadoopModuleFactory.class.getCanonicalName())) {
			return false;
		}
		// not compatible if Hadoop binary not in classpath.
		try {
			Class.forName(
				"org.apache.hadoop.security.UserGroupInformation",
				false,
				HadoopSecurityContextFactory.class.getClassLoader());
			return true;
		} catch (ClassNotFoundException e) {
			LOG.info("Cannot install HadoopSecurityContext because Hadoop cannot be found in the Classpath.");
			return false;
		}
	}

	@Override
	public SecurityContext createContext(SecurityConfiguration securityConfig) throws SecurityContextInitializeException {
		try {
			UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
			return new HadoopSecurityContext(loginUser);
		} catch (Exception e) {
			LOG.error("Cannot instantiate HadoopSecurityContext.", e);
			throw new SecurityContextInitializeException("Cannot instantiate HadoopSecurityContext.", e);
		}
	}
}
