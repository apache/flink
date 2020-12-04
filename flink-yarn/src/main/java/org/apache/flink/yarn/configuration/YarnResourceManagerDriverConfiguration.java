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

package org.apache.flink.yarn.configuration;

import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.YarnConfigKeys;

import org.apache.hadoop.yarn.api.ApplicationConstants;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * Configuration specific to {@link org.apache.flink.yarn.YarnResourceManagerDriver}.
 */
public class YarnResourceManagerDriverConfiguration {
	@Nullable private final String webInterfaceUrl;
	private final String rpcAddress;
	private final String yarnFiles;
	private final String flinkClasspath;
	private final String clientShipFiles;
	private final String flinkDistJar;
	private final String currentDir;
	@Nullable private final String remoteKeytabPath;
	@Nullable private final String localKeytabPath;
	@Nullable private final String keytabPrinciple;
	@Nullable private final String krb5Path;
	@Nullable private final String yarnSiteXMLPath;

	public YarnResourceManagerDriverConfiguration(
		Map<String, String> env,
		String rpcAddress,
		@Nullable String webInterfaceUrl) {
		this.rpcAddress = Preconditions.checkNotNull(rpcAddress);
		this.webInterfaceUrl = webInterfaceUrl;
		this.yarnFiles = Preconditions.checkNotNull(env.get(YarnConfigKeys.FLINK_YARN_FILES));
		this.flinkClasspath = Preconditions.checkNotNull(env.get(YarnConfigKeys.ENV_FLINK_CLASSPATH));
		this.clientShipFiles = Preconditions.checkNotNull(env.get(YarnConfigKeys.ENV_CLIENT_SHIP_FILES));
		this.flinkDistJar = Preconditions.checkNotNull(env.get(YarnConfigKeys.FLINK_DIST_JAR));
		this.remoteKeytabPath = env.get(YarnConfigKeys.REMOTE_KEYTAB_PATH);
		this.localKeytabPath = env.get(YarnConfigKeys.LOCAL_KEYTAB_PATH);
		this.keytabPrinciple = env.get(YarnConfigKeys.KEYTAB_PRINCIPAL);
		this.krb5Path = env.get(YarnConfigKeys.ENV_KRB5_PATH);
		this.yarnSiteXMLPath = env.get(YarnConfigKeys.ENV_YARN_SITE_XML_PATH);
		this.currentDir = Preconditions.checkNotNull(env.get(ApplicationConstants.Environment.PWD.key()));
	}

	public String getRpcAddress() {
		return rpcAddress;
	}

	@Nullable
	public String getWebInterfaceUrl() {
		return webInterfaceUrl;
	}

	public String getClientShipFiles() {
		return clientShipFiles;
	}

	public String getFlinkClasspath() {
		return flinkClasspath;
	}

	public String getFlinkDistJar() {
		return flinkDistJar;
	}

	@Nullable
	public String getKeytabPrinciple() {
		return keytabPrinciple;
	}

	@Nullable
	public String getKrb5Path() {
		return krb5Path;
	}

	@Nullable
	public String getLocalKeytabPath() {
		return localKeytabPath;
	}

	@Nullable
	public String getRemoteKeytabPath() {
		return remoteKeytabPath;
	}

	public String getYarnFiles() {
		return yarnFiles;
	}

	@Nullable
	public String getYarnSiteXMLPath() {
		return yarnSiteXMLPath;
	}

	public String getCurrentDir() {
		return currentDir;
	}
}
