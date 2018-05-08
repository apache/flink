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
import org.apache.flink.annotation.docs.ConfigGroup;
import org.apache.flink.annotation.docs.ConfigGroups;

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

	// ------------------------------------------------------------------------
	//  SSL Security Options
	// ------------------------------------------------------------------------

	/**
	 * Enable SSL support.
	 */
	public static final ConfigOption<Boolean> SSL_ENABLED =
		key("security.ssl.enabled")
			.defaultValue(false)
			.withDescription("Turns on SSL for internal network communication. This can be optionally overridden by" +
				" flags defined in different transport modules.");

	/**
	 * The Java keystore file containing the flink endpoint key and certificate.
	 */
	public static final ConfigOption<String> SSL_KEYSTORE =
		key("security.ssl.keystore")
			.noDefaultValue()
			.withDescription("The Java keystore file to be used by the flink endpoint for its SSL Key and Certificate.");

	/**
	 * Secret to decrypt the keystore file.
	 */
	public static final ConfigOption<String> SSL_KEYSTORE_PASSWORD =
		key("security.ssl.keystore-password")
			.noDefaultValue()
			.withDescription("The secret to decrypt the keystore file.");

	/**
	 * Secret to decrypt the server key.
	 */
	public static final ConfigOption<String> SSL_KEY_PASSWORD =
		key("security.ssl.key-password")
			.noDefaultValue()
			.withDescription("The secret to decrypt the server key in the keystore.");

	/**
	 * The truststore file containing the public CA certificates to verify the ssl peers.
	 */
	public static final ConfigOption<String> SSL_TRUSTSTORE =
		key("security.ssl.truststore")
			.noDefaultValue()
			.withDescription("The truststore file containing the public CA certificates to be used by flink endpoints" +
				" to verify the peer’s certificate.");

	/**
	 * Secret to decrypt the truststore.
	 */
	public static final ConfigOption<String> SSL_TRUSTSTORE_PASSWORD =
		key("security.ssl.truststore-password")
			.noDefaultValue()
			.withDescription("The secret to decrypt the truststore.");

	/**
	 * SSL protocol version to be supported.
	 */
	public static final ConfigOption<String> SSL_PROTOCOL =
		key("security.ssl.protocol")
			.defaultValue("TLSv1.2")
			.withDescription("The SSL protocol version to be supported for the ssl transport. Note that it doesn’t" +
				" support comma separated list.");

	/**
	 * The standard SSL algorithms to be supported.
	 *
	 * <p>More options here - http://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#ciphersuites
	 */
	public static final ConfigOption<String> SSL_ALGORITHMS =
		key("security.ssl.algorithms")
			.defaultValue("TLS_RSA_WITH_AES_128_CBC_SHA")
			.withDescription("The comma separated list of standard SSL algorithms to be supported. Read more" +
				" <a href=\"http://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#ciphersuites\">here</a>.");

	/**
	 * Flag to enable/disable hostname verification for the ssl connections.
	 */
	public static final ConfigOption<Boolean> SSL_VERIFY_HOSTNAME =
		key("security.ssl.verify-hostname")
			.defaultValue(true)
			.withDescription("Flag to enable peer’s hostname verification during ssl handshake.");
}
