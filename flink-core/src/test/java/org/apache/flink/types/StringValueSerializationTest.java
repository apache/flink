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

package org.apache.flink.types;

import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.StringUtils;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for the serialization of StringValue. */
class StringValueSerializationTest {

    private final Random rnd = new Random(2093486528937460234L);

    @Test
    void testNonNullValues() throws IOException {
        String[] testStrings =
                new String[] {"a", "", "bcd", "jbmbmner8 jhk hj \n \t üäßß@µ", "", "non-empty"};

        testSerialization(testStrings);
    }

    @Test
    void testLongValues() throws IOException {
        String[] testStrings =
                new String[] {
                    StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
                    StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
                    StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
                    StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2)
                };

        testSerialization(testStrings);
    }

    @Test
    void testMixedValues() throws IOException {
        String[] testStrings =
                new String[] {
                    StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
                    "",
                    StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
                    StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
                    "",
                    StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
                    ""
                };

        testSerialization(testStrings);
    }

    @Test
    void testBinaryCopyOfLongStrings() throws IOException {
        String[] testStrings =
                new String[] {
                    StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
                    "",
                    StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
                    StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
                    "",
                    StringUtils.getRandomString(rnd, 10000, 1024 * 1024 * 2),
                    ""
                };

        testCopy(testStrings);
    }

    public static void testSerialization(String[] values) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
        DataOutputViewStreamWrapper serializer = new DataOutputViewStreamWrapper(baos);

        for (String value : values) {
            StringValue sv = new StringValue(value);
            sv.write(serializer);
        }

        serializer.close();
        baos.close();

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputViewStreamWrapper deserializer = new DataInputViewStreamWrapper(bais);

        int num = 0;
        while (bais.available() > 0) {
            StringValue deser = new StringValue();
            deser.read(deserializer);

            assertThat(values[num]).isEqualTo(deser.getValue());
            num++;
        }

        assertThat(values).hasSize(num);
    }

    public static void testCopy(String[] values) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
        DataOutputViewStreamWrapper serializer = new DataOutputViewStreamWrapper(baos);

        StringValue sValue = new StringValue();

        for (String value : values) {
            sValue.setValue(value);
            sValue.write(serializer);
        }

        serializer.close();
        baos.close();

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputViewStreamWrapper source = new DataInputViewStreamWrapper(bais);

        ByteArrayOutputStream targetOutput = new ByteArrayOutputStream(4096);
        DataOutputViewStreamWrapper target = new DataOutputViewStreamWrapper(targetOutput);

        for (String value : values) {
            sValue.copy(source, target);
        }

        ByteArrayInputStream validateInput = new ByteArrayInputStream(targetOutput.toByteArray());
        DataInputViewStreamWrapper validate = new DataInputViewStreamWrapper(validateInput);

        int num = 0;
        while (validateInput.available() > 0) {
            sValue.read(validate);

            assertThat(values[num]).isEqualTo(sValue.getValue());
            num++;
        }

        assertThat(values).hasSize(num);
    }
}
