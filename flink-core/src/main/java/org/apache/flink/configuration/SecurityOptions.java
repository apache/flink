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
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.description.Description;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.LinkElement.link;

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
	 * Enable SSL for internal (rpc, data transport, blob server) and external (HTTP/REST) communication.
	 *
	 * @deprecated Use {@link #SSL_INTERNAL_ENABLED} and {@link #SSL_REST_ENABLED} instead.
	 */
	@Deprecated
	public static final ConfigOption<Boolean> SSL_ENABLED =
		key("security.ssl.enabled")
			.defaultValue(false)
			.withDescription("Turns on SSL for internal and external network communication." +
					"This can be overridden by 'security.ssl.internal.enabled', 'security.ssl.external.enabled'. " +
					"Specific internal components (rpc, data transport, blob server) may optionally override " +
					"this through their own settings.");

	/**
	 * Enable SSL for internal communication (akka rpc, netty data transport, blob server).
	 */
	@Documentation.CommonOption(position = Documentation.CommonOption.POSITION_SECURITY)
	public static final ConfigOption<Boolean> SSL_INTERNAL_ENABLED =
			key("security.ssl.internal.enabled")
			.defaultValue(false)
			.withDescription("Turns on SSL for internal network communication. " +
					"Optionally, specific components may override this through their own settings " +
					"(rpc, data transport, REST, etc).");

	/**
	 * Enable SSL for external REST endpoints.
	 */
	@Documentation.CommonOption(position = Documentation.CommonOption.POSITION_SECURITY)
	public static final ConfigOption<Boolean> SSL_REST_ENABLED =
			key("security.ssl.rest.enabled")
			.defaultValue(false)
			.withDescription("Turns on SSL for external communication via the REST endpoints.");

	/**
	 * Enable mututal SSL authentication for external REST endpoints.
	 */
	public static final ConfigOption<Boolean> SSL_REST_AUTHENTICATION_ENABLED =
		key("security.ssl.rest.authentication-enabled")
			.defaultValue(false)
			.withDescription("Turns on mutual SSL authentication for external communication via the REST endpoints.");

	// ----------------- certificates (internal + external) -------------------

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

	// ----------------------- certificates (internal) ------------------------

	/**
	 * For internal SSL, the Java keystore file containing the private key and certificate.
	 */
	public static final ConfigOption<String> SSL_INTERNAL_KEYSTORE =
			key("security.ssl.internal.keystore")
					.noDefaultValue()
					.withDescription("The Java keystore file with SSL Key and Certificate, " +
							"to be used Flink's internal endpoints (rpc, data transport, blob server).");

	/**
	 * For internal SSL, the password to decrypt the keystore file containing the certificate.
	 */
	public static final ConfigOption<String> SSL_INTERNAL_KEYSTORE_PASSWORD =
			key("security.ssl.internal.keystore-password")
					.noDefaultValue()
					.withDescription("The secret to decrypt the keystore file for Flink's " +
							"for Flink's internal endpoints (rpc, data transport, blob server).");

	/**
	 * For internal SSL, the password to decrypt the private key.
	 */
	public static final ConfigOption<String> SSL_INTERNAL_KEY_PASSWORD =
			key("security.ssl.internal.key-password")
					.noDefaultValue()
					.withDescription("The secret to decrypt the key in the keystore " +
							"for Flink's internal endpoints (rpc, data transport, blob server).");

	/**
	 * For internal SSL, the truststore file containing the public CA certificates to verify the ssl peers.
	 */
	public static final ConfigOption<String> SSL_INTERNAL_TRUSTSTORE =
			key("security.ssl.internal.truststore")
					.noDefaultValue()
					.withDescription("The truststore file containing the public CA certificates to verify the peer " +
							"for Flink's internal endpoints (rpc, data transport, blob server).");

	/**
	 * For internal SSL, the secret to decrypt the truststore.
	 */
	public static final ConfigOption<String> SSL_INTERNAL_TRUSTSTORE_PASSWORD =
			key("security.ssl.internal.truststore-password")
					.noDefaultValue()
					.withDescription("The password to decrypt the truststore " +
							"for Flink's internal endpoints (rpc, data transport, blob server).");

	// ----------------------- certificates (external) ------------------------

	/**
	 * For external (REST) SSL, the Java keystore file containing the private key and certificate.
	 */
	public static final ConfigOption<String> SSL_REST_KEYSTORE =
			key("security.ssl.rest.keystore")
					.noDefaultValue()
					.withDescription("The Java keystore file with SSL Key and Certificate, " +
							"to be used Flink's external REST endpoints.");

	/**
	 * For external (REST) SSL, the password to decrypt the keystore file containing the certificate.
	 */
	public static final ConfigOption<String> SSL_REST_KEYSTORE_PASSWORD =
			key("security.ssl.rest.keystore-password")
					.noDefaultValue()
					.withDescription("The secret to decrypt the keystore file for Flink's " +
							"for Flink's external REST endpoints.");

	/**
	 * For external (REST) SSL, the password to decrypt the private key.
	 */
	public static final ConfigOption<String> SSL_REST_KEY_PASSWORD =
			key("security.ssl.rest.key-password")
					.noDefaultValue()
					.withDescription("The secret to decrypt the key in the keystore " +
							"for Flink's external REST endpoints.");

	/**
	 * For external (REST) SSL, the truststore file containing the public CA certificates to verify the ssl peers.
	 */
	public static final ConfigOption<String> SSL_REST_TRUSTSTORE =
			key("security.ssl.rest.truststore")
					.noDefaultValue()
					.withDescription("The truststore file containing the public CA certificates to verify the peer " +
							"for Flink's external REST endpoints.");

	/**
	 * For external (REST) SSL, the secret to decrypt the truststore.
	 */
	public static final ConfigOption<String> SSL_REST_TRUSTSTORE_PASSWORD =
			key("security.ssl.rest.truststore-password")
					.noDefaultValue()
					.withDescription("The password to decrypt the truststore " +
							"for Flink's external REST endpoints.");

	// ------------------------ ssl parameters --------------------------------

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
			.withDescription(Description.builder()
				.text("The comma separated list of standard SSL algorithms to be supported. Read more %s",
					link(
						"http://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#ciphersuites",
						"here"))
				.build());

	/**
	 * Flag to enable/disable hostname verification for the ssl connections.
	 */
	public static final ConfigOption<Boolean> SSL_VERIFY_HOSTNAME =
		key("security.ssl.verify-hostname")
			.defaultValue(true)
			.withDescription("Flag to enable peer’s hostname verification during ssl handshake.");

	// ------------------------ ssl parameters --------------------------------

	/**
	 * SSL session cache size.
	 */
	public static final ConfigOption<Integer> SSL_INTERNAL_SESSION_CACHE_SIZE =
		key("security.ssl.internal.session-cache-size")
			.defaultValue(-1)
			.withDescription("The size of the cache used for storing SSL session objects. "
				+ "According to https://github.com/netty/netty/issues/832, you should always set "
				+ "this to an appropriate number to not run into a bug with stalling IO threads "
				+ "during garbage collection. (-1 = use system default).")
		.withDeprecatedKeys("security.ssl.session-cache-size");

	/**
	 * SSL session timeout.
	 */
	public static final ConfigOption<Integer> SSL_INTERNAL_SESSION_TIMEOUT =
		key("security.ssl.internal.session-timeout")
			.defaultValue(-1)
			.withDescription("The timeout (in ms) for the cached SSL session objects. (-1 = use system default)")
			.withDeprecatedKeys("security.ssl.session-timeout");

	/**
	 * SSL session timeout during handshakes.
	 */
	public static final ConfigOption<Integer> SSL_INTERNAL_HANDSHAKE_TIMEOUT =
		key("security.ssl.internal.handshake-timeout")
			.defaultValue(-1)
			.withDescription("The timeout (in ms) during SSL handshake. (-1 = use system default)")
			.withDeprecatedKeys("security.ssl.handshake-timeout");

	/**
	 * SSL session timeout after flushing the <tt>close_notify</tt> message.
	 */
	public static final ConfigOption<Integer> SSL_INTERNAL_CLOSE_NOTIFY_FLUSH_TIMEOUT =
		key("security.ssl.internal.close-notify-flush-timeout")
			.defaultValue(-1)
			.withDescription("The timeout (in ms) for flushing the `close_notify` that was triggered by closing a " +
				"channel. If the `close_notify` was not flushed in the given timeout the channel will be closed " +
				"forcibly. (-1 = use system default)")
			.withDeprecatedKeys("security.ssl.close-notify-flush-timeout");
}
