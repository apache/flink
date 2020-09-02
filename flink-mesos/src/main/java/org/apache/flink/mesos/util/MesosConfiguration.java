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

package org.apache.flink.mesos.util;

import org.apache.mesos.Protos;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import scala.Option;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The typed configuration settings associated with a Mesos scheduler.
 */
public class MesosConfiguration {

	private final String masterUrl;

	private final Protos.FrameworkInfo.Builder frameworkInfo;

	private final Option<Protos.Credential.Builder> credential;

	public MesosConfiguration(
		String masterUrl,
		Protos.FrameworkInfo.Builder frameworkInfo,
		Option<Protos.Credential.Builder> credential) {

		this.masterUrl = checkNotNull(masterUrl);
		this.frameworkInfo = checkNotNull(frameworkInfo);
		this.credential = checkNotNull(credential);
	}

	/**
	 * The Mesos connection string.
	 *
	 * <p>The value should be in one of the following forms:
	 * <pre>
	 * {@code
	 *     host:port
	 *     zk://host1:port1,host2:port2,.../path
	 *     zk://username:password@host1:port1,host2:port2,.../path
	 *     file:///path/to/file (where file contains one of the above)
	 * }
	 * </pre>
	 */
	public String masterUrl() {
		return masterUrl;
	}

	/**
	 * The framework registration info.
	 */
	public Protos.FrameworkInfo.Builder frameworkInfo() {
		return frameworkInfo;
	}

	/**
	 * The credential to authenticate the framework principal.
	 */
	public Option<Protos.Credential.Builder> credential() {
		return credential;
	}

	/**
	 * Revise the configuration with updated framework info.
	 */
	public MesosConfiguration withFrameworkInfo(Protos.FrameworkInfo.Builder frameworkInfo) {
		return new MesosConfiguration(masterUrl, frameworkInfo, credential);
	}

	/**
	 * Gets the roles associated with the framework.
	 */
	public Set<String> roles() {
		return frameworkInfo.hasRole() && !"*".equals(frameworkInfo.getRole()) ?
			Collections.singleton(frameworkInfo.getRole()) : Collections.emptySet();
	}

	@Override
	public String toString() {
		return "MesosConfiguration{" +
			"masterUrl='" + masterUrl + '\'' +
			", frameworkInfo=" + frameworkInfo +
			", credential=" + (credential.isDefined() ? "(not shown)" : "(none)") +
			'}';
	}

	/**
	 * A utility method to log relevant Mesos connection info.
	 */
	public static void logMesosConfig(Logger log, MesosConfiguration config) {

		Map<String, String> env = System.getenv();
		Protos.FrameworkInfo.Builder info = config.frameworkInfo();

		log.info("--------------------------------------------------------------------------------");
		log.info(" Mesos Info:");
		log.info("    Master URL: {}", config.masterUrl());

		log.info(" Framework Info:");
		log.info("    ID: {}", info.hasId() ? info.getId().getValue() : "(none)");
		log.info("    Name: {}", info.hasName() ? info.getName() : "(none)");
		log.info("    Failover Timeout (secs): {}", info.getFailoverTimeout());
		log.info("    Role: {}", info.hasRole() ? info.getRole() : "(none)");
		log.info("    Capabilities: {}",
			info.getCapabilitiesList().size() > 0 ? info.getCapabilitiesList() : "(none)");
		log.info("    Principal: {}", info.hasPrincipal() ? info.getPrincipal() : "(none)");
		log.info("    Host: {}", info.hasHostname() ? info.getHostname() : "(none)");
		if (env.containsKey("LIBPROCESS_IP")) {
			log.info("    LIBPROCESS_IP: {}", env.get("LIBPROCESS_IP"));
		}
		if (env.containsKey("LIBPROCESS_PORT")) {
			log.info("    LIBPROCESS_PORT: {}", env.get("LIBPROCESS_PORT"));
		}
		log.info("    Web UI: {}", info.hasWebuiUrl() ? info.getWebuiUrl() : "(none)");

		log.info("--------------------------------------------------------------------------------");

	}
}
