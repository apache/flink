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
package org.apache.flink.runtime.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedExceptionAction;

public class SecurityUtils {

	private static final Logger LOG = LoggerFactory.getLogger(SecurityUtils.class);

	// load Hadoop configuration when loading the security utils.
	private static Configuration hdConf = new Configuration();
	
	
	public static boolean isSecurityEnabled() {
		UserGroupInformation.setConfiguration(hdConf);
		return UserGroupInformation.isSecurityEnabled();
	}

	public static <T> T runSecured(final FlinkSecuredRunner<T> runner) throws Exception {
		UserGroupInformation.setConfiguration(hdConf);
		UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
		if (!ugi.hasKerberosCredentials()) {
			LOG.error("Security is enabled but no Kerberos credentials have been found. " +
						"You may authenticate using the kinit command.");
		}
		T ret = ugi.doAs(new PrivilegedExceptionAction<T>() {
			@Override
			public T run() throws Exception {
				return runner.run();
			}
		});
		return ret;
	}

	public static interface FlinkSecuredRunner<T> {
		public T run() throws Exception;
	}

}
