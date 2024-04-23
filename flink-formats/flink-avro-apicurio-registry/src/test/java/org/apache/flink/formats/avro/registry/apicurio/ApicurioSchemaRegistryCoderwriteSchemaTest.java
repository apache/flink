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

package org.apache.flink.formats.avro.registry.apicurio;

import org.apache.flink.configuration.ConfigOption;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;

import io.apicurio.registry.rest.v2.beans.ArtifactMetaData;
import org.apache.avro.Schema;
import org.apache.commons.collections.MapUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.USE_GLOBALID;
import static org.apache.flink.formats.avro.registry.apicurio.AvroApicurioFormatOptions.USE_HEADERS;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for schemacoder. */
public class ApicurioSchemaRegistryCoderwriteSchemaTest {

    @ParameterizedTest
    @MethodSource("configProvider")
    public void writeSchema(TestSpec testSpec) throws IOException {
        try {
            if (testSpec.avroFileName == null) {
                testSpec.avroFileName = "src/test/resources/simpleschema1.avsc";
            }

            // get schema from passed fileName contents
            String schemaStr = readFile(testSpec.avroFileName, StandardCharsets.UTF_8);
            Schema schema = new Schema.Parser().parse(schemaStr);
            // get config
            Map<String, Object> configs = getConfig(testSpec.isKey, testSpec.extraConfigOptions);
            // setup input
            Map<String, Object> inputProperties = new HashMap<>();
            inputProperties.put(ApicurioSchemaRegistryCoder.TOPIC_NAME, testSpec.topicName);
            inputProperties.put(ApicurioSchemaRegistryCoder.IS_KEY, testSpec.isKey);
            // setup output
            OutputStream out = new ByteArrayOutputStream();
            Map<String, Object> outputProperties = new HashMap<>();

            // create Mock registryClient
            MockRegistryClient registryClient = new MockRegistryClient();
            ArtifactMetaData artifactMetaData = new ArtifactMetaData();
            artifactMetaData.setContentId(testSpec.contentId);
            artifactMetaData.setGlobalId(testSpec.globalId);
            registryClient.setCreateArtifactResult(artifactMetaData);

            ApicurioSchemaRegistryCoder apicurioSchemaRegistryCoder =
                    new ApicurioSchemaRegistryCoder(registryClient, configs);

            apicurioSchemaRegistryCoder.writeSchema(schema, out, inputProperties, outputProperties);
            Map<String, Object> headersFromWriteSchema =
                    (Map<String, Object>) outputProperties.get(ApicurioSchemaRegistryCoder.HEADERS);

            if (testSpec.extraConfigOptions == null) {
                testSpec.extraConfigOptions = new HashMap<>();
            }
            if (testSpec.extraConfigOptions.get(USE_GLOBALID.key()) == null) {
                testSpec.extraConfigOptions.put(USE_GLOBALID.key(), true);
            }

            if (testSpec.extraConfigOptions.get(USE_HEADERS.key()) == null) {
                testSpec.extraConfigOptions.put(USE_HEADERS.key(), true);
            }

            boolean useGlobalId = (boolean) testSpec.extraConfigOptions.get(USE_GLOBALID.key());
            boolean useHeaders = (boolean) testSpec.extraConfigOptions.get(USE_HEADERS.key());

            Long testProducedId;

            if (useHeaders) {
                String expectedHeaderName;
                if (useGlobalId) {
                    if (testSpec.isKey) {
                        expectedHeaderName =
                                ApicurioSchemaRegistryCoder.APICURIO_KEY_GLOBAL_ID_HEADER;
                    } else {
                        expectedHeaderName =
                                ApicurioSchemaRegistryCoder.APICURIO_VALUE_GLOBAL_ID_HEADER;
                    }
                } else {
                    if (testSpec.isKey) {
                        expectedHeaderName =
                                ApicurioSchemaRegistryCoder.APICURIO_KEY_CONTENT_ID_HEADER;
                    } else {
                        expectedHeaderName =
                                ApicurioSchemaRegistryCoder.APICURIO_VALUE_CONTENT_ID_HEADER;
                    }
                }
                byte[] byteArrayFromHeaders =
                        (byte[]) headersFromWriteSchema.get(expectedHeaderName);
                assertThat(byteArrayFromHeaders).isNotEmpty();
                testProducedId = apicurioSchemaRegistryCoder.bytesToLong(byteArrayFromHeaders);
                byte[] byteArrayExpectedHeader =
                        (byte[]) testSpec.expectedHeaders.get(expectedHeaderName);
                if (!testSpec.expectSuccess && byteArrayExpectedHeader == null) {
                    // this is an error test so fail with an exception
                    throw new RuntimeException(
                            "Error test found an unexpected header with testSpec" + testSpec);
                }
                assertThat(byteArrayExpectedHeader).isNotEmpty();
                long expectedHeaderId =
                        apicurioSchemaRegistryCoder.bytesToLong(byteArrayExpectedHeader);
                if (!testSpec.expectSuccess) {
                    throw new RuntimeException(
                            "Error test: id in header is not expected with testSpec" + testSpec);
                }
                assertThat(testProducedId).isEqualTo(expectedHeaderId);
            } else {
                ByteArrayOutputStream baos = (ByteArrayOutputStream) out;
                byte[] bytes = baos.toByteArray();
                ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                DataInputStream dataInputStream = new DataInputStream(bais);
                // check for the magic byte
                assertThat(dataInputStream.readByte()).isEqualTo((byte) 0);
                testProducedId = dataInputStream.readLong();
            }
            long expectedId;
            if (useGlobalId) {
                expectedId = testSpec.globalId;
            } else {
                expectedId = testSpec.contentId;
            }
            assertThat(testProducedId).isEqualTo(expectedId);
            assertThat(testSpec.expectSuccess).isTrue();
        } catch (Exception e) {
            if (testSpec.expectSuccess) {
                throw new RuntimeException("Unexpected Error occurred with testSpec" + testSpec, e);
            }
        }
    }

    private static class TestSpec {

        boolean isKey;
        Map<String, Object> extraConfigOptions;
        String topicName;
        String avroFileName;
        long contentId;
        long globalId;
        Map<String, Object> expectedHeaders;
        boolean expectSuccess;

        private TestSpec(
                boolean isKey,
                Map<String, Object> extraConfigOptions,
                String topicName,
                String avroFileName,
                long contentId,
                long globalId,
                Map<String, Object> expectedHeaders,
                boolean expectSuccess) {
            this.extraConfigOptions = extraConfigOptions;
            this.isKey = isKey;
            this.topicName = topicName;
            this.avroFileName = avroFileName;
            this.contentId = contentId;
            this.globalId = globalId;
            this.expectedHeaders = expectedHeaders;
            this.expectSuccess = expectSuccess;
        }

        @Override
        public String toString() {
            String extraConfigOptionsStr = "";
            if (MapUtils.isNotEmpty(this.extraConfigOptions)) {
                for (String key : this.extraConfigOptions.keySet()) {
                    extraConfigOptionsStr =
                            extraConfigOptionsStr
                                    + "("
                                    + key
                                    + ","
                                    + this.extraConfigOptions.get(key)
                                    + ")\n";
                }
            }
            String headersStr = "";
            if (MapUtils.isNotEmpty(this.expectedHeaders)) {
                for (String key : this.expectedHeaders.keySet()) {
                    headersStr =
                            headersStr + "(" + key + "," + this.expectedHeaders.get(key) + ")\n";
                }
            }
            return "TestSpec{"
                    + "extraConfigOptions="
                    + extraConfigOptionsStr
                    + ", headers="
                    + headersStr
                    + ", isKey="
                    + isKey
                    + ", topicName="
                    + topicName
                    + ", avroFileName="
                    + avroFileName
                    + ", contentId="
                    + contentId
                    + ", globalId="
                    + globalId
                    + ", expectedHeaders="
                    + expectedHeaders
                    + ", expectSuccess="
                    + expectSuccess
                    + '}';
        }
    }

    private static class ValidTestSpec extends TestSpec {
        private ValidTestSpec(
                boolean isKey,
                Map<String, Object> extraConfigOptions,
                String topicName,
                String avroFileName,
                long contentId,
                long globalId,
                Map<String, Object> expectedHeaders) {
            super(
                    isKey,
                    extraConfigOptions,
                    topicName,
                    avroFileName,
                    contentId,
                    globalId,
                    expectedHeaders,
                    true);
        }
    }

    private static class InvalidTestSpec extends TestSpec {
        private InvalidTestSpec(
                boolean isKey,
                Map<String, Object> extraConfigOptions,
                String topicName,
                String avroFileName,
                long contentId,
                long globalId,
                Map<String, Object> expectedHeaders) {
            super(
                    isKey,
                    extraConfigOptions,
                    topicName,
                    avroFileName,
                    contentId,
                    globalId,
                    expectedHeaders,
                    false);
        }
    }

    static Collection<TestSpec> configProvider() {
        return ImmutableList.<TestSpec>builder()
                .addAll(getValidTestSpecs())
                .addAll(getInvalidTestSpecs())
                .build();
    }

    @NotNull
    private static ImmutableList<TestSpec> getValidTestSpecs() {
        return ImmutableList.of(
                // Key global
                new ValidTestSpec(
                        true,
                        null,
                        "topic1",
                        null,
                        12L,
                        13L,
                        ofEntries(
                                new String[] {
                                    ApicurioSchemaRegistryCoder.APICURIO_KEY_GLOBAL_ID_HEADER
                                },
                                new Object[] {ApicurioSchemaRegistryCoder.longToBytes(13)})),
                // value global
                new ValidTestSpec(
                        false,
                        null,
                        "topic1",
                        null,
                        12L,
                        13L,
                        ofEntries(
                                new String[] {
                                    ApicurioSchemaRegistryCoder.APICURIO_VALUE_GLOBAL_ID_HEADER
                                },
                                new Object[] {ApicurioSchemaRegistryCoder.longToBytes(13)})),
                // key content
                new ValidTestSpec(
                        true,
                        ofEntries(new String[] {USE_GLOBALID.key()}, new Object[] {false}),
                        "topic1",
                        null,
                        12L,
                        13L,
                        ofEntries(
                                new String[] {
                                    ApicurioSchemaRegistryCoder.APICURIO_KEY_CONTENT_ID_HEADER
                                },
                                new Object[] {ApicurioSchemaRegistryCoder.longToBytes(12)})),
                // value content
                new ValidTestSpec(
                        false,
                        ofEntries(new String[] {USE_GLOBALID.key()}, new Object[] {false}),
                        "topic1",
                        null,
                        12L,
                        13L,
                        ofEntries(
                                new String[] {
                                    ApicurioSchemaRegistryCoder.APICURIO_VALUE_CONTENT_ID_HEADER
                                },
                                new Object[] {ApicurioSchemaRegistryCoder.longToBytes(12)})),
                // Key global Legacy
                new ValidTestSpec(
                        true,
                        ofEntries(new String[] {USE_HEADERS.key()}, new Object[] {false}),
                        "topic1",
                        null,
                        12L,
                        13L,
                        null),
                // Value global Legacy
                new ValidTestSpec(
                        false,
                        ofEntries(new String[] {USE_HEADERS.key()}, new Object[] {false}),
                        "topic1",
                        null,
                        12L,
                        13L,
                        null),
                // Key content Legacy
                new ValidTestSpec(
                        true,
                        ofEntries(
                                new String[] {USE_HEADERS.key(), USE_GLOBALID.key()},
                                new Object[] {false, false}),
                        "topic1",
                        null,
                        12L,
                        13L,
                        null),
                // Value content Legacy
                new ValidTestSpec(
                        false,
                        ofEntries(
                                new String[] {USE_HEADERS.key(), USE_GLOBALID.key()},
                                new Object[] {false, false}),
                        "topic1",
                        null,
                        12L,
                        13L,
                        null));
    }

    private static byte[] getLegacyByteArray(long schemaId) {
        byte[] legacyByteArray = new byte[1 + Long.BYTES];
        ByteBuffer byteBuffer = ByteBuffer.wrap(legacyByteArray);
        // magic byte
        byteBuffer.put(new byte[] {0});
        // 8 byte schema id
        byteBuffer.put(ApicurioSchemaRegistryCoder.longToBytes(schemaId));
        return byteBuffer.array();
    }

    @NotNull
    private static ImmutableList<TestSpec> getInvalidTestSpecs() {
        return ImmutableList.of(
                // global key with value header
                new InvalidTestSpec(
                        true,
                        null,
                        "topic1",
                        null,
                        12L,
                        13L,
                        ofEntries(
                                new String[] {
                                    ApicurioSchemaRegistryCoder.APICURIO_VALUE_GLOBAL_ID_HEADER
                                },
                                new Object[] {ApicurioSchemaRegistryCoder.longToBytes(13L)})),
                // global key with mismatching content id in global header
                new InvalidTestSpec(
                        true,
                        null,
                        "topic1",
                        null,
                        12L,
                        13L,
                        ofEntries(
                                new String[] {
                                    ApicurioSchemaRegistryCoder.APICURIO_KEY_GLOBAL_ID_HEADER
                                },
                                new Object[] {ApicurioSchemaRegistryCoder.longToBytes(12)})),
                // global key with mismatching global id in content header
                new InvalidTestSpec(
                        true,
                        ofEntries(new String[] {USE_GLOBALID.key()}, new Object[] {false}),
                        "topic1",
                        null,
                        12L,
                        13L,
                        ofEntries(
                                new String[] {
                                    ApicurioSchemaRegistryCoder.APICURIO_KEY_CONTENT_ID_HEADER
                                },
                                new Object[] {ApicurioSchemaRegistryCoder.longToBytes(13)}))
                // Note policing Legacy mode with a header (when the id should be in
                // the payload)
                );
    }

    // Cannot use Map.of or Map.ofEntries as we are Java 8.
    private static Map<String, Object> ofEntries(String[] keys, Object[] values) {
        HashMap map = new HashMap();
        for (int i = 0; i < keys.length; i++) {
            map.put(keys[i], values[i]);
        }
        return map;
    }

    @NotNull
    private static Map<String, Object> getConfig(
            boolean isKey, // TODO do we need to handle value and key specific map keys or not?
            Map<String, Object> extraConfigOptions) {

        Set<ConfigOption<?>> configOptions = ApicurioRegistryAvroFormatFactory.getOptionalOptions();
        Map<String, Object> registryConfigs = new HashMap<>();
        for (ConfigOption configOption : configOptions) {
            Object value = null;
            String configOptionKey = configOption.key();
            if (extraConfigOptions != null) {
                value = extraConfigOptions.get(configOptionKey);
            }
            if (value == null && configOption.hasDefaultValue()) {
                value = configOption.defaultValue();
            }
            registryConfigs.put(configOptionKey, value);
        }
        return registryConfigs;
    }

    static String readFile(String path, Charset encoding) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }
}
