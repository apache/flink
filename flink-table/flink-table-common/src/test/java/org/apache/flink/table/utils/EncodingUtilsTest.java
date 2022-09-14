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

package org.apache.flink.table.utils;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link org.apache.flink.table.utils.EncodingUtils}. */
class EncodingUtilsTest {

    @Test
    void testObjectStringEncoding() {
        final MyPojo pojo = new MyPojo(33, "Hello");
        final String base64 = EncodingUtils.encodeObjectToString(pojo);
        assertThat(EncodingUtils.decodeStringToObject(base64, Serializable.class)).isEqualTo(pojo);
    }

    @Test
    void testStringBase64Encoding() {
        final String string = "Hello, this is apache flink.";
        final String base64 = EncodingUtils.encodeStringToBase64(string);
        assertThat(base64).isEqualTo("SGVsbG8sIHRoaXMgaXMgYXBhY2hlIGZsaW5rLg==");
        assertThat(EncodingUtils.decodeBase64ToString(base64)).isEqualTo(string);
    }

    @Test
    void testMd5Hex() {
        final String string = "Hello, world! How are you? 高精确";
        assertThat(EncodingUtils.hex(EncodingUtils.md5(string)))
                .isEqualTo("983abac84e994b4ba73be177e5cc298b");
    }

    @Test
    void testJavaEscaping() {
        assertThat(EncodingUtils.escapeJava("\\hello\"world'space/"))
                .isEqualTo("\\\\hello\\\"world'space/");
    }

    @Test
    void testRepetition() {
        assertThat(EncodingUtils.repeat("we", 3)).isEqualTo("wewewe");
    }

    // --------------------------------------------------------------------------------------------

    private static class MyPojo implements Serializable {

        private int number;
        private String string;

        public MyPojo(int number, String string) {
            this.number = number;
            this.string = string;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MyPojo myPojo = (MyPojo) o;
            return number == myPojo.number && Objects.equals(string, myPojo.string);
        }

        @Override
        public int hashCode() {
            return Objects.hash(number, string);
        }
    }
}
