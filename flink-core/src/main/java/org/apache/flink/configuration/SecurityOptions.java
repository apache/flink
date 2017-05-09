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
public class SecurityOptions {

	// ------------------------------------------------------------------------
	//  Kerberos Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<String> KERBEROS_LOGIN_PRINCIPAL =
		key("security.kerberos.login.principal")
			.noDefaultValue()
			.withDeprecatedKeys("security.principal");

	public static final ConfigOption<String> KERBEROS_LOGIN_KEYTAB =
		key("security.kerberos.login.keytab")
			.noDefaultValue()
			.withDeprecatedKeys("security.keytab");

	public static final ConfigOption<Boolean> KERBEROS_LOGIN_USETICKETCACHE =
		key("security.kerberos.login.use-ticket-cache")
			.defaultValue(true);

	public static final ConfigOption<String> KERBEROS_LOGIN_CONTEXTS =
		key("security.kerberos.login.contexts")
			.noDefaultValue();


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
			.defaultValue(false);

	/**
	 * The Java keystore file containing the flink endpoint key and certificate.
	 */
	public static final ConfigOption<String> SSL_KEYSTORE =
		key("security.ssl.keystore")
			.noDefaultValue();

	/**
	 * Secret to decrypt the keystore file.
	 */
	public static final ConfigOption<String> SSL_KEYSTORE_PASSWORD =
		key("security.ssl.keystore-password")
			.noDefaultValue();

	/**
	 * Secret to decrypt the server key.
	 */
	public static final ConfigOption<String> SSL_KEY_PASSWORD =
		key("security.ssl.key-password")
			.noDefaultValue();

	/**
	 * The truststore file containing the public CA certificates to verify the ssl peers.
	 */
	public static final ConfigOption<String> SSL_TRUSTSTORE =
		key("security.ssl.truststore")
			.noDefaultValue();

	/**
	 * Secret to decrypt the truststore.
	 */
	public static final ConfigOption<String> SSL_TRUSTSTORE_PASSWORD =
		key("security.ssl.truststore-password")
			.noDefaultValue();

	/**
	 * SSL protocol version to be supported.
	 */
	public static final ConfigOption<String> SSL_PROTOCOL =
		key("security.ssl.protocol")
			.defaultValue("TLSv1.2");

	/**
	 * The standard SSL algorithms to be supported.
	 *
	 * <p>More options here - http://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#ciphersuites
	 */
	public static final ConfigOption<String> SSL_ALGORITHMS =
		key("security.ssl.algorithms")
			.defaultValue("TLS_RSA_WITH_AES_128_CBC_SHA");

	/**
	 * Flag to enable/disable hostname verification for the ssl connections.
	 */
	public static final ConfigOption<Boolean> SSL_VERIFY_HOSTNAME =
		key("security.ssl.verify-hostname")
			.defaultValue(true);
}
