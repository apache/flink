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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * The set of configuration options relating to security.
 */
@PublicEvolving
@ConfigGroups(groups = {
	@ConfigGroup(name = "Kerberos", keyPrefix = "security.kerberos"),
	@ConfigGroup(name = "ZooKeeper", keyPrefix = "zookeeper")
	})
public class SecurityOptions {

	// ------------------------------------------------------------------------
	//  Kerberos Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<String> KERBEROS_LOGIN_PRINCIPAL =
		key("security.kerberos.login.principal")
			.noDefaultValue()
			.withDeprecatedKeys("security.principal")
			.withDescription("Kerberos principal name associated with the keytab.");

	public static final ConfigOption<String> KERBEROS_LOGIN_KEYTAB =
		key("security.kerberos.login.keytab")
			.noDefaultValue()
			.withDeprecatedKeys("security.keytab")
			.withDescription("Absolute path to a Kerberos keytab file that contains the user credentials.");

	public static final ConfigOption<Boolean> KERBEROS_LOGIN_USETICKETCACHE =
		key("security.kerberos.login.use-ticket-cache")
			.defaultValue(true)
			.withDescription("Indicates whether to read from your Kerberos ticket cache.");

	public static final ConfigOption<String> KERBEROS_LOGIN_CONTEXTS =
		key("security.kerberos.login.contexts")
			.noDefaultValue()
			.withDescription("A comma-separated list of login contexts to provide the Kerberos credentials to" +
				" (for example, `Client,KafkaClient` to use the credentials for ZooKeeper authentication and for" +
				" Kafka authentication)");


	// ------------------------------------------------------------------------
	//  ZooKeeper Security Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<Boolean> ZOOKEEPER_SASL_DISABLE =
		key("zookeeper.sasl.disable")
			.defaultValue(false);

	public static final ConfigOption<String> ZOOKEEPER_SASL_SERVICE_NAME =
		key("zookeeper.sasl.service-name")
			.defaultValue("zookeeper");

	public static final ConfigOption<String> ZOOKEEPER_SASL_LOGIN_CONTEXT_NAME =
		key("zookeeper.sasl.login-context-name")
			.defaultValue("Client");
}
