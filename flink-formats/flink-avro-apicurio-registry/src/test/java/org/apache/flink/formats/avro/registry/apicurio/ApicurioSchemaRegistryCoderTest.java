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

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/** Test for schemacoder. */
public class ApicurioSchemaRegistryCoderTest {
    @Test
    public void readSchemaWithHeaders() throws IOException {
        Map<String, Object> registryConfigs = new HashMap<>();
        registryConfigs.put(AvroApicurioFormatOptions.ENABLE_HEADERS.key(), true);
        // TODO either populate with Mockito instead?
        MockRegistryClient registryClient = new MockRegistryClient();
        Schema schemaFromFile =
                new Schema.Parser().parse(new File("src/test/resources/simpleschema1.avsc"));
        // populate with anything as it needs content
        String str = readFile("src/test/resources/simple1.avro", StandardCharsets.UTF_8);
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);

        InputStream in = new ByteArrayInputStream(bytes);
        registryClient.setInputStream(in);
        ApicurioSchemaRegistryCoder apicurioSchemaRegistryCoder =
                new ApicurioSchemaRegistryCoder(registryClient, registryConfigs);
        Map<String, Object> headers = new HashMap<>();
        headers.put(ApicurioSchemaRegistryCoder.APICURIO_VALUE_GLOBAL_ID, longToBytes(1));
        //        Schema schema = apicurioSchemaRegistryCoder.readSchemaWithHeaders(in, headers);
        //        assertThat(schemaFromFile.equals(schema));
    }

    public byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    static String readFile(String path, Charset encoding) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }
}
