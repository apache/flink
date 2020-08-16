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

package org.apache.flink.hadoop.utils;

import org.apache.flink.util.Preconditions;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Utility class for working with Hadoop security configurations. This should only be used if Hadoop
 * is on the classpath.
 */
public class HadoopSecurityUtils {

	private static final Logger LOG = LoggerFactory.getLogger(HadoopSecurityUtils.class);

	static final Text HDFS_DELEGATION_TOKEN_KIND = new Text("HDFS_DELEGATION_TOKEN");

	public static boolean isKerberosSecurityEnabled(UserGroupInformation ugi) {
		return UserGroupInformation.isSecurityEnabled() && ugi.getAuthenticationMethod() == UserGroupInformation.AuthenticationMethod.KERBEROS;
	}

	public static boolean areKerberosCredentialsValid(UserGroupInformation ugi, boolean useTicketCache) {
		Preconditions.checkState(isKerberosSecurityEnabled(ugi));

		// note: UGI::hasKerberosCredentials inaccurately reports false
		// for logins based on a keytab (fixed in Hadoop 2.6.1, see HADOOP-10786),
		// so we check only in ticket cache scenario.
		if (useTicketCache && !ugi.hasKerberosCredentials()) {
			if (hasHDFSDelegationToken(ugi)) {
				LOG.warn("Hadoop security is enabled but current login user does not have Kerberos credentials, " +
					"use delegation token instead. Flink application will terminate after token expires.");
				return true;
			} else {
				LOG.error("Hadoop security is enabled, but current login user has neither Kerberos credentials " +
					"nor delegation tokens!");
				return false;
			}
		}

		return true;
	}

	/**
	 * Indicates whether the user has an HDFS delegation token.
	 */
	public static boolean hasHDFSDelegationToken(UserGroupInformation ugi) {
		Collection<Token<? extends TokenIdentifier>> usrTok = ugi.getTokens();
		for (Token<? extends TokenIdentifier> token : usrTok) {
			if (token.getKind().equals(HDFS_DELEGATION_TOKEN_KIND)) {
				return true;
			}
		}
		return false;
	}
}
