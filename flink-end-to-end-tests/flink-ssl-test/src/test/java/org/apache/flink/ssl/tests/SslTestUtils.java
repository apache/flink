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

package org.apache.flink.ssl.tests;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.tests.util.AutoClosableProcess.runBlocking;

/**
 * Utility class for SSL test setup and certificate generation. This class provides Java-based
 * alternatives to the common_ssl.sh bash scripts used in end-to-end tests.
 */
public class SslTestUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SslTestUtils.class);

    /** SSL provider types. */
    public enum SslProvider {
        JDK,
        OPENSSL
    }

    /** SSL provider library linking type. */
    public enum ProviderLibrary {
        DYNAMIC,
        STATIC
    }

    /** SSL connectivity type. */
    public enum SslType {
        INTERNAL("internal"),
        REST("rest");

        private final String value;

        SslType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    /** SSL authentication mode. */
    public enum AuthenticationMode {
        SERVER,
        MUTUAL
    }

    /**
     * Generates SSL certificates and configures Flink SSL settings.
     *
     * @param testDataDir the root test data directory
     * @param type SSL type (internal or rest)
     * @param provider SSL provider (JDK or OPENSSL)
     * @param providerLib provider library type (dynamic or static)
     * @return Configuration with SSL settings
     * @throws IOException if certificate generation fails
     */
    public static Configuration setupSslHelper(
            Path testDataDir, SslType type, SslProvider provider, ProviderLibrary providerLib)
            throws IOException {
        return setupSslHelper(testDataDir, type, provider, providerLib, 2);
    }

    /**
     * Generates SSL certificates and configures Flink SSL settings with custom validity period.
     *
     * @param testDataDir the root test data directory
     * @param type SSL type (internal or rest)
     * @param provider SSL provider (JDK or OPENSSL)
     * @param providerLib provider library type (dynamic or static)
     * @param validityDays certificate validity period in days
     * @return Configuration with SSL settings
     * @throws IOException if certificate generation fails
     */
    public static Configuration setupSslHelper(
            Path testDataDir,
            SslType type,
            SslProvider provider,
            ProviderLibrary providerLib,
            int validityDays)
            throws IOException {

        LOG.info(
                "Setting up SSL with: {} {} {} (validity: {} days)",
                type,
                provider,
                providerLib,
                validityDays);

        Path sslDir = testDataDir.resolve("ssl").resolve(type.getValue());
        String password = type.getValue() + ".password";

        // Generate and install certificates
        generateAndInstallCertificates(sslDir, password, validityDays);

        // Configure OpenSSL if needed
        if (provider == SslProvider.OPENSSL) {
            configureOpenSsl(providerLib);
        }

        // Build and return configuration
        return buildSslConfiguration(type, provider, sslDir, password);
    }

    /**
     * Generates SSL certificates and installs them in the specified directory.
     *
     * @param sslDir the directory where certificates will be stored
     * @param password the password for keystores
     * @param validityDays certificate validity period in days
     * @throws IOException if certificate generation fails
     */
    public static void generateAndInstallCertificates(
            Path sslDir, String password, int validityDays) throws IOException {

        // Clean up and create SSL directory
        if (Files.exists(sslDir)) {
            LOG.info("Directory {} exists. Deleting it...", sslDir);
            deleteRecursively(sslDir);
        }
        Files.createDirectories(sslDir);

        // Build SAN string
        String nodeName = getNodeName();
        List<String> nodeIps = getNodeIps();
        StringBuilder sanString = new StringBuilder("dns:" + nodeName);
        for (String ip : nodeIps) {
            sanString.append(",ip:").append(ip);
        }

        LOG.info("Using SAN {}", sanString);

        // Create certificates
        createCertificates(sslDir, password, nodeName, sanString.toString(), validityDays);

        // Export keystore to PEM format for curl
        convertKeystoreToPem(sslDir, password);
    }

    /** Sets up internal SSL configuration. */
    public static Configuration setupInternalSsl(
            Path testDataDir, SslProvider provider, ProviderLibrary providerLib)
            throws IOException {
        return setupSslHelper(testDataDir, SslType.INTERNAL, provider, providerLib);
    }

    /** Sets up REST SSL configuration. */
    public static Configuration setupRestSsl(
            Path testDataDir,
            AuthenticationMode auth,
            SslProvider provider,
            ProviderLibrary providerLib)
            throws IOException {
        Configuration config = setupSslHelper(testDataDir, SslType.REST, provider, providerLib);

        boolean mutualAuth = auth == AuthenticationMode.MUTUAL;
        LOG.info("Mutual ssl auth: {}", mutualAuth);
        config.set(SecurityOptions.SSL_REST_AUTHENTICATION_ENABLED, mutualAuth);

        return config;
    }

    /**
     * Creates SSL certificates using keytool.
     *
     * @param sslDir the directory where certificates will be stored
     * @param password the password for keystores
     * @param nodeName the node hostname
     * @param sanString the Subject Alternative Names string
     * @param validityDays certificate validity period in days
     * @throws IOException if certificate generation fails
     */
    private static void createCertificates(
            Path sslDir, String password, String nodeName, String sanString, int validityDays)
            throws IOException {

        // Generate CA certificate
        runBlocking(
                "keytool",
                "-genkeypair",
                "-alias",
                "ca",
                "-keystore",
                sslDir.resolve("ca.keystore").toString(),
                "-dname",
                "CN=Sample CA",
                "-storepass",
                password,
                "-keypass",
                password,
                "-keyalg",
                "RSA",
                "-ext",
                "bc=ca:true",
                "-storetype",
                "PKCS12",
                "-validity",
                String.valueOf(validityDays));

        // Export CA certificate
        runBlocking(
                "keytool",
                "-keystore",
                sslDir.resolve("ca.keystore").toString(),
                "-storepass",
                password,
                "-alias",
                "ca",
                "-exportcert",
                "-file",
                sslDir.resolve("ca.cer").toString());

        // Import CA certificate to truststore
        runBlocking(
                "keytool",
                "-importcert",
                "-keystore",
                sslDir.resolve("ca.truststore").toString(),
                "-alias",
                "ca",
                "-storepass",
                password,
                "-noprompt",
                "-file",
                sslDir.resolve("ca.cer").toString());

        // Generate node certificate
        runBlocking(
                "keytool",
                "-genkeypair",
                "-alias",
                "node",
                "-keystore",
                sslDir.resolve("node.keystore").toString(),
                "-dname",
                "CN=" + nodeName,
                "-ext",
                "SAN=" + sanString,
                "-storepass",
                password,
                "-keypass",
                password,
                "-keyalg",
                "RSA",
                "-storetype",
                "PKCS12",
                "-validity",
                String.valueOf(validityDays));

        // Create certificate signing request
        runBlocking(
                "keytool",
                "-certreq",
                "-keystore",
                sslDir.resolve("node.keystore").toString(),
                "-storepass",
                password,
                "-alias",
                "node",
                "-file",
                sslDir.resolve("node.csr").toString());

        // Sign certificate
        runBlocking(
                "keytool",
                "-gencert",
                "-keystore",
                sslDir.resolve("ca.keystore").toString(),
                "-storepass",
                password,
                "-alias",
                "ca",
                "-ext",
                "SAN=" + sanString,
                "-validity",
                String.valueOf(validityDays),
                "-infile",
                sslDir.resolve("node.csr").toString(),
                "-outfile",
                sslDir.resolve("node.cer").toString());

        // Import CA certificate to node keystore
        runBlocking(
                "keytool",
                "-importcert",
                "-keystore",
                sslDir.resolve("node.keystore").toString(),
                "-storepass",
                password,
                "-file",
                sslDir.resolve("ca.cer").toString(),
                "-alias",
                "ca",
                "-noprompt");

        // Import signed node certificate
        runBlocking(
                "keytool",
                "-importcert",
                "-keystore",
                sslDir.resolve("node.keystore").toString(),
                "-storepass",
                password,
                "-file",
                sslDir.resolve("node.cer").toString(),
                "-alias",
                "node",
                "-noprompt");
    }

    /** Converts keystore to PEM format using OpenSSL. */
    private static void convertKeystoreToPem(Path sslDir, String password) throws IOException {
        List<String> command = new ArrayList<>();
        command.add("openssl");
        command.add("pkcs12");

        // Check OpenSSL version and add legacy flag if needed
        if (isOpenSsl3OrHigher()) {
            command.add("-legacy");
        }

        command.add("-passin");
        command.add("pass:" + password);
        command.add("-in");
        command.add(sslDir.resolve("node.keystore").toString());
        command.add("-out");
        command.add(sslDir.resolve("node.pem").toString());
        command.add("-nodes");

        runBlocking(command.toArray(new String[0]));
    }

    /**
     * Gets certificate validity dates from a given host and port.
     *
     * @param host the host to check
     * @param port the port to check
     * @return array with [notBefore, notAfter] date strings, or null if unable to retrieve
     */
    public static String[] getCertificateValidityDates(String host, int port) {
        try {
            ProcessBuilder pb =
                    new ProcessBuilder(
                            "sh",
                            "-c",
                            String.format(
                                    "openssl s_client -connect %s:%d </dev/null 2>/dev/null | openssl x509 -noout -dates",
                                    host, port));

            Process process = pb.start();
            StringBuilder output = new StringBuilder();

            try (var reader =
                    new java.io.BufferedReader(
                            new java.io.InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append("\n");
                }
            }

            process.waitFor(5, java.util.concurrent.TimeUnit.SECONDS);
            process.destroyForcibly();

            String result = output.toString();
            String notBefore = null;
            String notAfter = null;

            // Parse output
            for (String line : result.split("\n")) {
                if (line.startsWith("notBefore=")) {
                    notBefore = line.substring("notBefore=".length()).trim();
                } else if (line.startsWith("notAfter=")) {
                    notAfter = line.substring("notAfter=".length()).trim();
                }
            }

            if (notBefore != null && notAfter != null) {
                LOG.info(
                        "Certificate validity for {}:{} - notBefore: {}, notAfter: {}",
                        host,
                        port,
                        notBefore,
                        notAfter);
                return new String[] {notBefore, notAfter};
            }

            LOG.warn("Could not retrieve certificate validity dates from {}:{}", host, port);
            return null;
        } catch (Exception e) {
            LOG.debug("Failed to get certificate dates from {}:{}", host, port, e);
            return null;
        }
    }

    /** Checks if OpenSSL version is 3.x or higher. */
    private static boolean isOpenSsl3OrHigher() {
        try {
            Process process = new ProcessBuilder("openssl", "version").start();
            try (var reader =
                    new java.io.BufferedReader(
                            new java.io.InputStreamReader(process.getInputStream()))) {
                String version = reader.readLine();
                if (version != null) {
                    return !version.contains("OpenSSL 1");
                }
            }
            process.waitFor();
        } catch (Exception e) {
            LOG.warn("Could not determine OpenSSL version, assuming OpenSSL 3+", e);
        }
        return true;
    }

    /**
     * Configures OpenSSL library for Flink.
     *
     * @param providerLib the provider library type (dynamic or static)
     */
    private static void configureOpenSsl(ProviderLibrary providerLib) {
        // This would copy the appropriate netty-tcnative jar to Flink's lib directory
        // For test purposes, this might not be needed if using JDK provider
        LOG.info("OpenSSL configuration for {} library type would be applied here", providerLib);
        // Implementation depends on test environment setup
        // In bash script this copies flink-shaded-netty-tcnative-*.jar to $FLINK_DIR/lib/
    }

    /** Builds Flink SSL configuration. */
    private static Configuration buildSslConfiguration(
            SslType type, SslProvider provider, Path sslDir, String password) {

        Configuration config = new Configuration();

        config.set(SecurityOptions.SSL_PROVIDER, provider.name());

        if (type == SslType.INTERNAL) {
            config.set(SecurityOptions.SSL_INTERNAL_ENABLED, true);
            config.set(
                    SecurityOptions.SSL_INTERNAL_KEYSTORE,
                    sslDir.resolve("node.keystore").toString());
            config.set(SecurityOptions.SSL_INTERNAL_KEYSTORE_PASSWORD, password);
            config.set(SecurityOptions.SSL_INTERNAL_KEY_PASSWORD, password);
            config.set(
                    SecurityOptions.SSL_INTERNAL_TRUSTSTORE,
                    sslDir.resolve("ca.truststore").toString());
            config.set(SecurityOptions.SSL_INTERNAL_TRUSTSTORE_PASSWORD, password);
        } else { // REST
            config.set(SecurityOptions.SSL_REST_ENABLED, true);
            config.set(
                    SecurityOptions.SSL_REST_KEYSTORE, sslDir.resolve("node.keystore").toString());
            config.set(SecurityOptions.SSL_REST_KEYSTORE_PASSWORD, password);
            config.set(SecurityOptions.SSL_REST_KEY_PASSWORD, password);
            config.set(
                    SecurityOptions.SSL_REST_TRUSTSTORE,
                    sslDir.resolve("ca.truststore").toString());
            config.set(SecurityOptions.SSL_REST_TRUSTSTORE_PASSWORD, password);
        }

        return config;
    }

    /** Gets the node name (hostname). */
    private static String getNodeName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            LOG.warn("Could not determine hostname, using localhost", e);
            return "localhost";
        }
    }

    /** Gets all IP addresses of the node. */
    private static List<String> getNodeIps() {
        List<String> ips = new ArrayList<>();
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface iface = interfaces.nextElement();
                if (iface.isLoopback() || !iface.isUp()) {
                    continue;
                }
                Enumeration<InetAddress> addresses = iface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress addr = addresses.nextElement();
                    ips.add(addr.getHostAddress());
                }
            }
        } catch (Exception e) {
            LOG.warn("Could not enumerate network interfaces, using localhost", e);
        }
        if (ips.isEmpty()) {
            ips.add("127.0.0.1");
        }
        return ips;
    }

    /** Recursively deletes a directory. */
    private static void deleteRecursively(Path path) throws IOException {
        if (Files.isDirectory(path)) {
            try (var stream = Files.list(path)) {
                for (Path child : stream.collect(Collectors.toList())) {
                    deleteRecursively(child);
                }
            }
        }
        Files.deleteIfExists(path);
    }
}
