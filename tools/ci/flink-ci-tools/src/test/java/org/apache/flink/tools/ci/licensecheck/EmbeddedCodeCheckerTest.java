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

package org.apache.flink.tools.ci.licensecheck;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link EmbeddedCodeChecker}, run through {@link JarFileChecker#checkJar}. */
class EmbeddedCodeCheckerTest {

    private static final String FLINK_NOTICE =
            "Flink : Test\nCopyright 2014-2026 The Apache Software Foundation\n\n";
    private static final String NETTY_NOTICE = "Copyright 2014 The Netty Project\n";
    private static final String UTF8_VALIDATOR =
            "org/apache/flink/shaded/netty4/io/netty/handler/codec/http/websocketx/Utf8Validator.class";
    private static final String UTF8_LICENSE = "META-INF/licenses/LICENSE.hoehrmann-utf8.txt";
    private static final String UTF8_LICENSE_TEXT =
            "Copyright (c) 2008-2009 Bjoern Hoehrmann <bjoern@hoehrmann.de>";

    @Test
    void testCodeWithItsLicenseAccepted(@TempDir Path tempDir) throws Exception {
        assertThat(
                        check(
                                tempDir,
                                FLINK_NOTICE + NETTY_NOTICE,
                                UTF8_VALIDATOR,
                                "",
                                UTF8_LICENSE,
                                UTF8_LICENSE_TEXT))
                .isZero();
    }

    @Test
    void testCodeWithoutItsLicenseRejected(@TempDir Path tempDir) throws Exception {
        assertThat(check(tempDir, FLINK_NOTICE + NETTY_NOTICE, UTF8_VALIDATOR, "")).isOne();
    }

    @Test
    void testLicenseWithoutItsCodeRejected(@TempDir Path tempDir) throws Exception {
        assertThat(check(tempDir, FLINK_NOTICE, UTF8_LICENSE, UTF8_LICENSE_TEXT)).isOne();
    }

    @Test
    void testLicenseWithWrongTextRejected(@TempDir Path tempDir) throws Exception {
        assertThat(
                        check(
                                tempDir,
                                FLINK_NOTICE + NETTY_NOTICE,
                                UTF8_VALIDATOR,
                                "",
                                UTF8_LICENSE,
                                "Copyright (c) 2010-2011 Matthew J. Francis"))
                .isOne();
    }

    @Test
    void testMarkerFoundUnderAnyRelocation(@TempDir Path tempDir) throws Exception {
        for (String prefix :
                Arrays.asList(
                        "",
                        "io/grpc/netty/shaded/",
                        "org/apache/beam/vendor/grpc/v1p69p0/",
                        "com/facebook/presto/hadoop/$internal/")) {
            assertThat(
                            check(
                                    tempDir,
                                    FLINK_NOTICE + NETTY_NOTICE,
                                    prefix + "io/netty/handler/codec/compression/FastLz.class",
                                    ""))
                    .as(prefix)
                    .isOne();
        }
    }

    @Test
    void testMissingNoticeAttributionRejected(@TempDir Path tempDir) throws Exception {
        final String netUtil = "io/netty/util/NetUtil.class";
        assertThat(check(tempDir, FLINK_NOTICE + NETTY_NOTICE, netUtil, "")).isOne();
        assertThat(
                        check(
                                tempDir,
                                FLINK_NOTICE
                                        + NETTY_NOTICE
                                        + "a modified portion of 'Apache\n"
                                        + "Harmony', an open source Java SE\n",
                                netUtil,
                                ""))
                .isZero();
        // Netty's own copyright line is owed by any jar with Netty code in it
        assertThat(check(tempDir, FLINK_NOTICE, netUtil, "")).isEqualTo(2);
    }

    @Test
    void testNativeLibraryWithoutLicenseRejected(@TempDir Path tempDir) throws Exception {
        final String tcnative =
                "META-INF/native/libio_grpc_netty_shaded_netty_tcnative_linux_x86_64.so";
        final String tcnativeNotice = "Copyright 2016 The Netty Project\nTomcat Native\n";
        assertThat(check(tempDir, FLINK_NOTICE + tcnativeNotice, tcnative, "")).isOne();
        assertThat(
                        check(
                                tempDir,
                                FLINK_NOTICE + tcnativeNotice,
                                tcnative,
                                "",
                                "META-INF/licenses/LICENSE.boringssl.txt",
                                "Apache License"))
                .isZero();
        assertThat(
                        check(
                                tempDir,
                                FLINK_NOTICE,
                                tcnative,
                                "",
                                "META-INF/licenses/LICENSE.boringssl.txt",
                                "Apache License"))
                .isEqualTo(2);
    }

    @Test
    void testOpenSslLicenseRequiresAcknowledgment(@TempDir Path tempDir) throws Exception {
        final String conscrypt = "META-INF/native/libconscrypt_openjdk_jni-linux-x86_64.so";
        final String license = "META-INF/licenses/LICENSE.boringssl-openssl.txt";
        final String text = "BoringSSL is a fork of OpenSSL.";
        assertThat(check(tempDir, FLINK_NOTICE, conscrypt, "", license, text)).isEqualTo(3);
        assertThat(
                        check(
                                tempDir,
                                FLINK_NOTICE
                                        + "This product includes software developed by the OpenSSL Project\n"
                                        + "This product includes cryptographic software written by\n"
                                        + "Eric Young (eay@cryptsoft.com)\n"
                                        + "This product includes software written by\n"
                                        + "Tim Hudson (tjh@cryptsoft.com)\n",
                                conscrypt,
                                "",
                                license,
                                text))
                .isZero();
    }

    @Test
    void testTcnativeClassesNeedItsNoticeWithoutNatives(@TempDir Path tempDir) throws Exception {
        final String sslClass = "alluxio/shaded/client/io/netty/internal/tcnative/SSL.class";
        assertThat(check(tempDir, FLINK_NOTICE + NETTY_NOTICE, sslClass, "")).isEqualTo(2);
        assertThat(
                        check(
                                tempDir,
                                FLINK_NOTICE
                                        + NETTY_NOTICE
                                        + "netty-tcnative\nCopyright 2016 The Netty Project\n"
                                        + "This product contains a forked and modified version of Tomcat Native\n",
                                sslClass,
                                ""))
                .isZero();
    }

    @Test
    void testUnknownNettyNativeLibraryRejected(@TempDir Path tempDir) throws Exception {
        assertThat(
                        check(
                                tempDir,
                                FLINK_NOTICE,
                                "META-INF/native/libnetty_transport_native_epoll_x86_64.so",
                                ""))
                .isZero();
        assertThat(
                        check(
                                tempDir,
                                FLINK_NOTICE,
                                "META-INF/native/libnetty_codec_native_zstd_x86_64.so",
                                ""))
                .isOne();
    }

    @Test
    void testCodeInNestedJarJustifiesLicense(@TempDir Path tempDir) throws Exception {
        final byte[] nestedJar = jarBytes(entries(UTF8_VALIDATOR, ""));
        assertThat(
                        check(
                                tempDir,
                                FLINK_NOTICE,
                                UTF8_LICENSE,
                                UTF8_LICENSE_TEXT,
                                "flink-rpc-akka.jar",
                                nestedJar))
                .isZero();
    }

    @Test
    void testUnverifiedNettyVersionRejected(@TempDir Path tempDir) throws Exception {
        final String versions = "META-INF/io.netty.versions.properties";
        assertThat(
                        check(
                                tempDir,
                                FLINK_NOTICE,
                                versions,
                                "netty-common.version=4.1.100.Final\n"
                                        + "netty-tcnative-classes.version=2.0.99.Final\n"))
                .isZero();
        assertThat(
                        check(
                                tempDir,
                                FLINK_NOTICE,
                                versions,
                                "netty-common.version=4.2.999.Final\n"
                                        + "netty-buffer.version=4.2.999.Final\n"))
                .isOne();
        assertThat(check(tempDir, FLINK_NOTICE, versions, "netty-common.version=5.0.0.Final\n"))
                .isOne();
        assertThat(
                        check(
                                tempDir,
                                FLINK_NOTICE,
                                versions,
                                "netty-common.version=4.2.999.Final-SNAPSHOT\n"))
                .isOne();
        assertThat(
                        check(
                                tempDir,
                                FLINK_NOTICE,
                                "META-INF/io.grpc.netty.shaded.io.netty.versions.properties",
                                "netty-common.version=4.1.999.Final\n"))
                .isOne();
    }

    @Test
    void testTwitterHpackNeedsItsOwnAttribution(@TempDir Path tempDir) throws Exception {
        final String hpackDecoder = "io/netty/handler/codec/http2/HpackDecoder.class";
        assertThat(
                        check(
                                tempDir,
                                FLINK_NOTICE + NETTY_NOTICE + "- com.twitter:chill-java:0.7.6\n",
                                hpackDecoder,
                                ""))
                .isOne();
        assertThat(
                        check(
                                tempDir,
                                FLINK_NOTICE
                                        + NETTY_NOTICE
                                        + "  * https://github.com/twitter/hpack\n",
                                hpackDecoder,
                                ""))
                .isZero();
    }

    @Test
    void testRulesParsing() {
        final EmbeddedCodeChecker.Rules rules =
                EmbeddedCodeChecker.Rules.parse(
                        Arrays.asList(
                                "# comment",
                                "verified-netty = 4.1.10.Final",
                                "[a]",
                                "marker = x/.*",
                                "license = LICENSE.a.txt",
                                "license-text = A",
                                "notice = N",
                                "[natives]",
                                "native = META-INF/native/.*"));
        assertThat(rules.rules).hasSize(1);
        assertThat(rules.rules.get(0).licenses).containsExactly("LICENSE.a.txt");
        assertThat(rules.nativePatterns).hasSize(1);
        assertThat(rules.isVerified(new int[] {4, 1, 10})).isTrue();
        assertThat(rules.isVerified(new int[] {4, 1, 11})).isFalse();
        assertThat(rules.isVerified(new int[] {4, 2, 0})).isFalse();

        assertThatThrownBy(
                        () -> EmbeddedCodeChecker.Rules.parse(Arrays.asList("[a]", "unknown = 1")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EmbeddedCodeChecker.Rules.parse(Arrays.asList("marker = x")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testShippedRulesLoad() {
        final EmbeddedCodeChecker.Rules rules =
                EmbeddedCodeChecker.Rules.load("embedded-code.rules");
        assertThat(rules.rules).isNotEmpty();
        assertThat(rules.rules)
                .allSatisfy(
                        rule ->
                                assertThat(rule.licenses.isEmpty() && rule.noticeTexts.isEmpty())
                                        .as(rule.name)
                                        .isFalse());
        assertThat(rules.verifiedVersionsDescription()).isNotEmpty();
    }

    /**
     * Builds a jar with a valid Flink LICENSE, the given NOTICE and the given entries (name,
     * contents pairs; contents are a String or byte[]), and returns the number of severe issues
     * {@link JarFileChecker} finds in it.
     */
    private static int check(Path tempDir, String notice, Object... namesAndContents)
            throws Exception {
        final Map<String, Object> entries = entries(namesAndContents);
        entries.put("META-INF/NOTICE", notice);
        entries.put("META-INF/LICENSE", "Apache License\nVersion 2.0, January 2004\n");
        final Path jar = tempDir.resolve(UUID.randomUUID() + ".jar");
        Files.write(jar, jarBytes(entries));
        return JarFileChecker.checkJar(jar);
    }

    private static Map<String, Object> entries(Object... namesAndContents) {
        final Map<String, Object> entries = new LinkedHashMap<>();
        for (int i = 0; i < namesAndContents.length; i += 2) {
            entries.put((String) namesAndContents[i], namesAndContents[i + 1]);
        }
        return entries;
    }

    private static byte[] jarBytes(Map<String, Object> entries) throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ZipOutputStream zip = new ZipOutputStream(bytes)) {
            for (Map.Entry<String, Object> entry : entries.entrySet()) {
                zip.putNextEntry(new ZipEntry(entry.getKey()));
                write(zip, entry.getValue());
                zip.closeEntry();
            }
        }
        return bytes.toByteArray();
    }

    private static void write(OutputStream out, Object contents) throws IOException {
        out.write(
                contents instanceof byte[]
                        ? (byte[]) contents
                        : ((String) contents).getBytes(StandardCharsets.UTF_8));
    }
}
