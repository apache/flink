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

import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.description.Description;

import java.util.List;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.LineBreakElement.linebreak;
import static org.apache.flink.configuration.description.LinkElement.link;
import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;

/** The set of configuration options relating to security. */
public class SecurityOptions {

    // ------------------------------------------------------------------------
    //  Custom Security Service Loader
    // ------------------------------------------------------------------------

    public static final ConfigOption<List<String>> SECURITY_CONTEXT_FACTORY_CLASSES =
            key("security.context.factory.classes")
                    .stringType()
                    .asList()
                    .defaultValues(
                            "org.apache.flink.runtime.security.contexts.HadoopSecurityContextFactory",
                            "org.apache.flink.runtime.security.contexts.NoOpSecurityContextFactory")
                    .withDescription(
                            "List of factories that should be used to instantiate a security context. "
                                    + "If multiple are configured, Flink will use the first compatible "
                                    + "factory. You should have a NoOpSecurityContextFactory in this list "
                                    + "as a fallback.");

    public static final ConfigOption<List<String>> SECURITY_MODULE_FACTORY_CLASSES =
            key("security.module.factory.classes")
                    .stringType()
                    .asList()
                    .defaultValues(
                            "org.apache.flink.runtime.security.modules.HadoopModuleFactory",
                            "org.apache.flink.runtime.security.modules.JaasModuleFactory",
                            "org.apache.flink.runtime.security.modules.ZookeeperModuleFactory")
                    .withDescription(
                            "List of factories that should be used to instantiate security "
                                    + "modules. All listed modules will be installed. Keep in mind that the "
                                    + "configured security context might rely on some modules being present.");

    // ------------------------------------------------------------------------
    //  Kerberos Options
    // ------------------------------------------------------------------------

    @Documentation.Section(Documentation.Sections.SECURITY_AUTH_KERBEROS)
    public static final ConfigOption<String> KERBEROS_LOGIN_PRINCIPAL =
            key("security.kerberos.login.principal")
                    .stringType()
                    .noDefaultValue()
                    .withDeprecatedKeys("security.principal")
                    .withDescription("Kerberos principal name associated with the keytab.");

    @Documentation.Section(Documentation.Sections.SECURITY_AUTH_KERBEROS)
    public static final ConfigOption<String> KERBEROS_LOGIN_KEYTAB =
            key("security.kerberos.login.keytab")
                    .stringType()
                    .noDefaultValue()
                    .withDeprecatedKeys("security.keytab")
                    .withDescription(
                            "Absolute path to a Kerberos keytab file that contains the user credentials.");

    public static final ConfigOption<String> KERBEROS_KRB5_PATH =
            key("security.kerberos.krb5-conf.path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Specify the local location of the krb5.conf file. If defined, this conf would be mounted on the JobManager and "
                                    + "TaskManager containers/pods for Kubernetes, Yarn and Mesos. Note: The KDC defined needs to be visible from inside the containers.");

    @Documentation.Section(Documentation.Sections.SECURITY_AUTH_KERBEROS)
    public static final ConfigOption<Boolean> KERBEROS_LOGIN_USETICKETCACHE =
            key("security.kerberos.login.use-ticket-cache")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Indicates whether to read from your Kerberos ticket cache.");

    @Documentation.Section(Documentation.Sections.SECURITY_AUTH_KERBEROS)
    public static final ConfigOption<String> KERBEROS_LOGIN_CONTEXTS =
            key("security.kerberos.login.contexts")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A comma-separated list of login contexts to provide the Kerberos credentials to"
                                    + " (for example, `Client,KafkaClient` to use the credentials for ZooKeeper authentication and for"
                                    + " Kafka authentication)");

    @Documentation.Section(Documentation.Sections.SECURITY_AUTH_KERBEROS)
    public static final ConfigOption<Boolean> KERBEROS_FETCH_DELEGATION_TOKEN =
            key("security.kerberos.fetch.delegation-token")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Indicates whether to fetch the delegation tokens for external services the Flink job needs to contact. "
                                    + "Only HDFS and HBase are supported. It is used in Yarn deployments. "
                                    + "If true, Flink will fetch HDFS and HBase delegation tokens and inject them into Yarn AM containers. "
                                    + "If false, Flink will assume that the delegation tokens are managed outside of Flink. "
                                    + "As a consequence, it will not fetch delegation tokens for HDFS and HBase. "
                                    + "You may need to disable this option, if you rely on submission mechanisms, e.g. Apache Oozie, "
                                    + "to handle delegation tokens.");

    // ------------------------------------------------------------------------
    //  ZooKeeper Security Options
    // ------------------------------------------------------------------------

    @Documentation.Section(Documentation.Sections.SECURITY_AUTH_ZOOKEEPER)
    public static final ConfigOption<Boolean> ZOOKEEPER_SASL_DISABLE =
            key("zookeeper.sasl.disable").booleanType().defaultValue(false);

    @Documentation.Section(Documentation.Sections.SECURITY_AUTH_ZOOKEEPER)
    public static final ConfigOption<String> ZOOKEEPER_SASL_SERVICE_NAME =
            key("zookeeper.sasl.service-name").stringType().defaultValue("zookeeper");

    @Documentation.Section(Documentation.Sections.SECURITY_AUTH_ZOOKEEPER)
    public static final ConfigOption<String> ZOOKEEPER_SASL_LOGIN_CONTEXT_NAME =
            key("zookeeper.sasl.login-context-name").stringType().defaultValue("Client");

    // ------------------------------------------------------------------------
    //  SSL Security Options
    // ------------------------------------------------------------------------

    /**
     * Enable SSL for internal (rpc, data transport, blob server) and external (HTTP/REST)
     * communication.
     *
     * @deprecated Use {@link #SSL_INTERNAL_ENABLED} and {@link #SSL_REST_ENABLED} instead.
     */
    @Deprecated
    public static final ConfigOption<Boolean> SSL_ENABLED =
            key("security.ssl.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Turns on SSL for internal and external network communication."
                                    + "This can be overridden by 'security.ssl.internal.enabled', 'security.ssl.external.enabled'. "
                                    + "Specific internal components (rpc, data transport, blob server) may optionally override "
                                    + "this through their own settings.");

    /** Enable SSL for internal communication (akka rpc, netty data transport, blob server). */
    @Documentation.Section(Documentation.Sections.SECURITY_SSL)
    public static final ConfigOption<Boolean> SSL_INTERNAL_ENABLED =
            key("security.ssl.internal.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Turns on SSL for internal network communication. "
                                    + "Optionally, specific components may override this through their own settings "
                                    + "(rpc, data transport, REST, etc).");

    /** Enable SSL for external REST endpoints. */
    @Documentation.Section(Documentation.Sections.SECURITY_SSL)
    public static final ConfigOption<Boolean> SSL_REST_ENABLED =
            key("security.ssl.rest.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Turns on SSL for external communication via the REST endpoints.");

    /** Enable mututal SSL authentication for external REST endpoints. */
    @Documentation.Section(Documentation.Sections.SECURITY_SSL)
    public static final ConfigOption<Boolean> SSL_REST_AUTHENTICATION_ENABLED =
            key("security.ssl.rest.authentication-enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Turns on mutual SSL authentication for external communication via the REST endpoints.");

    // ----------------- certificates (internal + external) -------------------

    /** The Java keystore file containing the flink endpoint key and certificate. */
    @Documentation.ExcludeFromDocumentation(
            "The SSL Setup encourages separate configs for internal and REST security.")
    public static final ConfigOption<String> SSL_KEYSTORE =
            key("security.ssl.keystore")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The Java keystore file to be used by the flink endpoint for its SSL Key and Certificate.");

    /** Secret to decrypt the keystore file. */
    @Documentation.ExcludeFromDocumentation(
            "The SSL Setup encourages separate configs for internal and REST security.")
    public static final ConfigOption<String> SSL_KEYSTORE_PASSWORD =
            key("security.ssl.keystore-password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The secret to decrypt the keystore file.");

    /** Secret to decrypt the server key. */
    @Documentation.ExcludeFromDocumentation(
            "The SSL Setup encourages separate configs for internal and REST security.")
    public static final ConfigOption<String> SSL_KEY_PASSWORD =
            key("security.ssl.key-password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The secret to decrypt the server key in the keystore.");

    /** The truststore file containing the public CA certificates to verify the ssl peers. */
    @Documentation.ExcludeFromDocumentation(
            "The SSL Setup encourages separate configs for internal and REST security.")
    public static final ConfigOption<String> SSL_TRUSTSTORE =
            key("security.ssl.truststore")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The truststore file containing the public CA certificates to be used by flink endpoints"
                                    + " to verify the peer’s certificate.");

    /** Secret to decrypt the truststore. */
    @Documentation.ExcludeFromDocumentation(
            "The SSL Setup encourages separate configs for internal and REST security.")
    public static final ConfigOption<String> SSL_TRUSTSTORE_PASSWORD =
            key("security.ssl.truststore-password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The secret to decrypt the truststore.");

    // ----------------------- certificates (internal) ------------------------

    /** For internal SSL, the Java keystore file containing the private key and certificate. */
    @Documentation.Section(Documentation.Sections.SECURITY_SSL)
    public static final ConfigOption<String> SSL_INTERNAL_KEYSTORE =
            key("security.ssl.internal.keystore")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The Java keystore file with SSL Key and Certificate, "
                                    + "to be used Flink's internal endpoints (rpc, data transport, blob server).");

    /** For internal SSL, the password to decrypt the keystore file containing the certificate. */
    @Documentation.Section(Documentation.Sections.SECURITY_SSL)
    public static final ConfigOption<String> SSL_INTERNAL_KEYSTORE_PASSWORD =
            key("security.ssl.internal.keystore-password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The secret to decrypt the keystore file for Flink's "
                                    + "for Flink's internal endpoints (rpc, data transport, blob server).");

    /** For internal SSL, the password to decrypt the private key. */
    @Documentation.Section(Documentation.Sections.SECURITY_SSL)
    public static final ConfigOption<String> SSL_INTERNAL_KEY_PASSWORD =
            key("security.ssl.internal.key-password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The secret to decrypt the key in the keystore "
                                    + "for Flink's internal endpoints (rpc, data transport, blob server).");

    /**
     * For internal SSL, the truststore file containing the public CA certificates to verify the ssl
     * peers.
     */
    @Documentation.Section(Documentation.Sections.SECURITY_SSL)
    public static final ConfigOption<String> SSL_INTERNAL_TRUSTSTORE =
            key("security.ssl.internal.truststore")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The truststore file containing the public CA certificates to verify the peer "
                                    + "for Flink's internal endpoints (rpc, data transport, blob server).");

    /** For internal SSL, the secret to decrypt the truststore. */
    @Documentation.Section(Documentation.Sections.SECURITY_SSL)
    public static final ConfigOption<String> SSL_INTERNAL_TRUSTSTORE_PASSWORD =
            key("security.ssl.internal.truststore-password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The password to decrypt the truststore "
                                    + "for Flink's internal endpoints (rpc, data transport, blob server).");

    /** For internal SSL, the sha1 fingerprint of the internal certificate to verify the client. */
    @Documentation.Section(Documentation.Sections.SECURITY_SSL)
    public static final ConfigOption<String> SSL_INTERNAL_CERT_FINGERPRINT =
            key("security.ssl.internal.cert.fingerprint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The sha1 fingerprint of the internal certificate. "
                                    + "This further protects the internal communication to present the exact certificate used by Flink."
                                    + "This is necessary where one cannot use private CA(self signed) or there is internal firm wide CA is required");

    // ----------------------- certificates (external) ------------------------

    /**
     * For external (REST) SSL, the Java keystore file containing the private key and certificate.
     */
    @Documentation.Section(Documentation.Sections.SECURITY_SSL)
    public static final ConfigOption<String> SSL_REST_KEYSTORE =
            key("security.ssl.rest.keystore")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The Java keystore file with SSL Key and Certificate, "
                                    + "to be used Flink's external REST endpoints.");

    /**
     * For external (REST) SSL, the password to decrypt the keystore file containing the
     * certificate.
     */
    @Documentation.Section(Documentation.Sections.SECURITY_SSL)
    public static final ConfigOption<String> SSL_REST_KEYSTORE_PASSWORD =
            key("security.ssl.rest.keystore-password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The secret to decrypt the keystore file for Flink's "
                                    + "for Flink's external REST endpoints.");

    /** For external (REST) SSL, the password to decrypt the private key. */
    @Documentation.Section(Documentation.Sections.SECURITY_SSL)
    public static final ConfigOption<String> SSL_REST_KEY_PASSWORD =
            key("security.ssl.rest.key-password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The secret to decrypt the key in the keystore "
                                    + "for Flink's external REST endpoints.");

    /**
     * For external (REST) SSL, the truststore file containing the public CA certificates to verify
     * the ssl peers.
     */
    @Documentation.Section(Documentation.Sections.SECURITY_SSL)
    public static final ConfigOption<String> SSL_REST_TRUSTSTORE =
            key("security.ssl.rest.truststore")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The truststore file containing the public CA certificates to verify the peer "
                                    + "for Flink's external REST endpoints.");

    /** For external (REST) SSL, the secret to decrypt the truststore. */
    @Documentation.Section(Documentation.Sections.SECURITY_SSL)
    public static final ConfigOption<String> SSL_REST_TRUSTSTORE_PASSWORD =
            key("security.ssl.rest.truststore-password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The password to decrypt the truststore "
                                    + "for Flink's external REST endpoints.");

    /** For external (REST) SSL, the sha1 fingerprint of the rest client certificate to verify. */
    @Documentation.Section(Documentation.Sections.SECURITY_SSL)
    public static final ConfigOption<String> SSL_REST_CERT_FINGERPRINT =
            key("security.ssl.rest.cert.fingerprint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The sha1 fingerprint of the rest certificate. "
                                    + "This further protects the rest REST endpoints to present certificate which is only used by proxy server"
                                    + "This is necessary where once uses public CA or internal firm wide CA");

    // ------------------------ ssl parameters --------------------------------

    /** SSL protocol version to be supported. */
    @Documentation.Section(Documentation.Sections.SECURITY_SSL)
    public static final ConfigOption<String> SSL_PROTOCOL =
            key("security.ssl.protocol")
                    .stringType()
                    .defaultValue("TLSv1.2")
                    .withDescription(
                            "The SSL protocol version to be supported for the ssl transport. Note that it doesn’t"
                                    + " support comma separated list.");

    /**
     * The standard SSL algorithms to be supported.
     *
     * <p>More options here -
     * http://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#ciphersuites
     */
    @Documentation.Section(Documentation.Sections.SECURITY_SSL)
    public static final ConfigOption<String> SSL_ALGORITHMS =
            key("security.ssl.algorithms")
                    .stringType()
                    .defaultValue("TLS_RSA_WITH_AES_128_CBC_SHA")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The comma separated list of standard SSL algorithms to be supported. Read more %s",
                                            link(
                                                    "http://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#ciphersuites",
                                                    "here"))
                                    .build());

    /** Flag to enable/disable hostname verification for the ssl connections. */
    @Documentation.Section(Documentation.Sections.SECURITY_SSL)
    public static final ConfigOption<Boolean> SSL_VERIFY_HOSTNAME =
            key("security.ssl.verify-hostname")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Flag to enable peer’s hostname verification during ssl handshake.");

    /** SSL engine provider. */
    @Documentation.Section(Documentation.Sections.EXPERT_SECURITY_SSL)
    public static final ConfigOption<String> SSL_PROVIDER =
            key("security.ssl.provider")
                    .stringType()
                    .defaultValue("JDK")
                    .withDescription(
                            Description.builder()
                                    .text("The SSL engine provider to use for the ssl transport:")
                                    .list(
                                            text("%s: default Java-based SSL engine", code("JDK")),
                                            text(
                                                    "%s: openSSL-based SSL engine using system libraries",
                                                    code("OPENSSL")))
                                    .text(
                                            "%s is based on %s and comes in two flavours:",
                                            code("OPENSSL"),
                                            link(
                                                    "http://netty.io/wiki/forked-tomcat-native.html#wiki-h2-4",
                                                    "netty-tcnative"))
                                    .list(
                                            text(
                                                    "dynamically linked: This will use your system's openSSL libraries "
                                                            + "(if compatible) and requires %s to be copied to %s",
                                                    code(
                                                            "opt/flink-shaded-netty-tcnative-dynamic-*.jar"),
                                                    code("lib/")),
                                            text(
                                                    "statically linked: Due to potential licensing issues with "
                                                            + "openSSL (see %s), we cannot ship pre-built libraries. However, "
                                                            + "you can build the required library yourself and put it into %s:%s%s",
                                                    link(
                                                            "https://issues.apache.org/jira/browse/LEGAL-393",
                                                            "LEGAL-393"),
                                                    code("lib/"),
                                                    linebreak(),
                                                    code(
                                                            "git clone https://github.com/apache/flink-shaded.git && "
                                                                    + "cd flink-shaded && "
                                                                    + "mvn clean package -Pinclude-netty-tcnative-static -pl flink-shaded-netty-tcnative-static")))
                                    .build());

    // ------------------------ ssl parameters --------------------------------

    /** SSL session cache size. */
    @Documentation.Section(Documentation.Sections.EXPERT_SECURITY_SSL)
    public static final ConfigOption<Integer> SSL_INTERNAL_SESSION_CACHE_SIZE =
            key("security.ssl.internal.session-cache-size")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The size of the cache used for storing SSL session objects. "
                                                    + "According to %s, you should always set "
                                                    + "this to an appropriate number to not run into a bug with stalling IO threads "
                                                    + "during garbage collection. (-1 = use system default).",
                                            link(
                                                    "https://github.com/netty/netty/issues/832",
                                                    "here"))
                                    .build())
                    .withDeprecatedKeys("security.ssl.session-cache-size");

    /** SSL session timeout. */
    @Documentation.Section(Documentation.Sections.EXPERT_SECURITY_SSL)
    public static final ConfigOption<Integer> SSL_INTERNAL_SESSION_TIMEOUT =
            key("security.ssl.internal.session-timeout")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "The timeout (in ms) for the cached SSL session objects. (-1 = use system default)")
                    .withDeprecatedKeys("security.ssl.session-timeout");

    /** SSL session timeout during handshakes. */
    @Documentation.Section(Documentation.Sections.EXPERT_SECURITY_SSL)
    public static final ConfigOption<Integer> SSL_INTERNAL_HANDSHAKE_TIMEOUT =
            key("security.ssl.internal.handshake-timeout")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "The timeout (in ms) during SSL handshake. (-1 = use system default)")
                    .withDeprecatedKeys("security.ssl.handshake-timeout");

    /** SSL session timeout after flushing the <tt>close_notify</tt> message. */
    @Documentation.Section(Documentation.Sections.EXPERT_SECURITY_SSL)
    public static final ConfigOption<Integer> SSL_INTERNAL_CLOSE_NOTIFY_FLUSH_TIMEOUT =
            key("security.ssl.internal.close-notify-flush-timeout")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "The timeout (in ms) for flushing the `close_notify` that was triggered by closing a "
                                    + "channel. If the `close_notify` was not flushed in the given timeout the channel will be closed "
                                    + "forcibly. (-1 = use system default)")
                    .withDeprecatedKeys("security.ssl.close-notify-flush-timeout");
}
