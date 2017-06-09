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
package org.apache.flink.runtime.security.modules;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.hadoop.mapred.utils.HadoopUtils;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Responsible for installing a Hadoop login user.
 */
public class HadoopModule implements SecurityModule {

	private static final Logger LOG = LoggerFactory.getLogger(HadoopModule.class);

	UserGroupInformation loginUser;

	@Override
	public void install(SecurityUtils.SecurityConfiguration securityConfig) throws SecurityInstallException {

		UserGroupInformation.setConfiguration(securityConfig.getHadoopConfiguration());

		try {
			if (UserGroupInformation.isSecurityEnabled() &&
				!StringUtils.isBlank(securityConfig.getKeytab()) && !StringUtils.isBlank(securityConfig.getPrincipal())) {
				String keytabPath = (new File(securityConfig.getKeytab())).getAbsolutePath();

				UserGroupInformation.loginUserFromKeytab(securityConfig.getPrincipal(), keytabPath);

				loginUser = UserGroupInformation.getLoginUser();

				// supplement with any available tokens
				String fileLocation = System.getenv(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
				if (fileLocation != null) {
					/*
					 * Use reflection API since the API semantics are not available in Hadoop1 profile. Below APIs are
					 * used in the context of reading the stored tokens from UGI.
					 * Credentials cred = Credentials.readTokenStorageFile(new File(fileLocation), config.hadoopConf);
					 * loginUser.addCredentials(cred);
					*/
					try {
						Method readTokenStorageFileMethod = Credentials.class.getMethod("readTokenStorageFile",
							File.class, org.apache.hadoop.conf.Configuration.class);
						Credentials cred = (Credentials) readTokenStorageFileMethod.invoke(null, new File(fileLocation),
							securityConfig.getHadoopConfiguration());
						Method addCredentialsMethod = UserGroupInformation.class.getMethod("addCredentials",
							Credentials.class);
						addCredentialsMethod.invoke(loginUser, cred);
					} catch (NoSuchMethodException e) {
						LOG.warn("Could not find method implementations in the shaded jar. Exception: {}", e);
					} catch (InvocationTargetException e) {
						throw e.getTargetException();
					}
				}
			} else {
				// login with current user credentials (e.g. ticket cache, OS login)
				// note that the stored tokens are read automatically
				try {
					//Use reflection API to get the login user object
					//UserGroupInformation.loginUserFromSubject(null);
					Method loginUserFromSubjectMethod = UserGroupInformation.class.getMethod("loginUserFromSubject", Subject.class);
					loginUserFromSubjectMethod.invoke(null, (Subject) null);
				} catch (NoSuchMethodException e) {
					LOG.warn("Could not find method implementations in the shaded jar. Exception: {}", e);
				} catch (InvocationTargetException e) {
					throw e.getTargetException();
				}

				loginUser = UserGroupInformation.getLoginUser();
			}

			if (UserGroupInformation.isSecurityEnabled()) {
				// note: UGI::hasKerberosCredentials inaccurately reports false
				// for logins based on a keytab (fixed in Hadoop 2.6.1, see HADOOP-10786),
				// so we check only in ticket cache scenario.
				if (securityConfig.useTicketCache() && !loginUser.hasKerberosCredentials()) {
					// a delegation token is an adequate substitute in most cases
					if (!HadoopUtils.hasHDFSDelegationToken()) {
						LOG.warn("Hadoop security is enabled but current login user does not have Kerberos credentials");
					}
				}
			}

			LOG.info("Hadoop user set to {}", loginUser);

		} catch (Throwable ex) {
			throw new SecurityInstallException("Unable to set the Hadoop login user", ex);
		}
	}

	@Override
	public void uninstall() throws SecurityInstallException {
		throw new UnsupportedOperationException();
	}
}
