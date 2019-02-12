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

package org.apache.flink.runtime.security.factories;

import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.contexts.HadoopSecurityContext;
import org.apache.flink.runtime.security.contexts.NoOpSecurityContext;
import org.apache.flink.runtime.security.contexts.SecurityContext;

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Default security context factory that instantiates {@link SecurityContext}
 * based on installed modules, it would instantiate {@link HadoopSecurityContext} if
 * a {@link HadoopModuleFactory} is included.
 */
public class DefaultSecurityContextFactory implements SecurityContextFactory {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultSecurityContextFactory.class);

	@Override
	public SecurityContext createContext(SecurityConfiguration securityConfig) {
		// First check if we have installed hadoop security module and if Hadoop is in the ClassPath.
		// If required by the context but doesn't exist in ClassPath, we return default NoOpContext
		if (securityConfig.getSecurityModuleFactories().contains(HadoopModuleFactory.class.getCanonicalName())) {
			try {
				Class.forName(
					"org.apache.hadoop.security.UserGroupInformation",
					false,
					DefaultSecurityContextFactory.class.getClassLoader());
				UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
				return new HadoopSecurityContext(loginUser);
			} catch (ClassNotFoundException e) {
				LOG.info("Cannot install HadoopSecurityContext because Hadoop cannot be found in the Classpath.");
			} catch (LinkageError | IOException e) {
				LOG.error("Cannot install HadoopSecurityContext.", e);
			}
			return new NoOpSecurityContext();
		}
		else {
			return new NoOpSecurityContext();
		}
	}
}
