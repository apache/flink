/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for guarding {@link CompositeKeySerializationUtils}. */
class CompositeKeySerializationUtilsTest {

    @Test
    void testIsAmbiguousKeyPossible() {
        assertThat(
                        CompositeKeySerializationUtils.isAmbiguousKeyPossible(
                                IntSerializer.INSTANCE, IntSerializer.INSTANCE))
                .isFalse();

        assertThat(
                        CompositeKeySerializationUtils.isAmbiguousKeyPossible(
                                StringSerializer.INSTANCE, StringSerializer.INSTANCE))
                .isTrue();
    }

    @Test
    void testKeyGroupSerializationAndDeserialization() throws Exception {
        ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos(8);
        DataOutputView outputView = new DataOutputViewStreamWrapper(outputStream);

        for (int keyGroupPrefixBytes = 1; keyGroupPrefixBytes <= 2; ++keyGroupPrefixBytes) {
            for (int orgKeyGroup = 0; orgKeyGroup < 128; ++orgKeyGroup) {
                outputStream.reset();
                CompositeKeySerializationUtils.writeKeyGroup(
                        orgKeyGroup, keyGroupPrefixBytes, outputView);
                int deserializedKeyGroup =
                        CompositeKeySerializationUtils.readKeyGroup(
                                keyGroupPrefixBytes,
                                new DataInputViewStreamWrapper(
                                        new ByteArrayInputStreamWithPos(
                                                outputStream.toByteArray())));
                assertThat(deserializedKeyGroup).isEqualTo(orgKeyGroup);
            }
        }
    }

    @Test
    void testKeySerializationAndDeserialization() throws Exception {
        final DataOutputSerializer outputView = new DataOutputSerializer(8);
        final DataInputDeserializer inputView = new DataInputDeserializer();

        // test for key
        for (int orgKey = 0; orgKey < 100; ++orgKey) {
            outputView.clear();
            CompositeKeySerializationUtils.writeKey(
                    orgKey, IntSerializer.INSTANCE, outputView, false);
            inputView.setBuffer(outputView.getCopyOfBuffer());
            int deserializedKey =
                    CompositeKeySerializationUtils.readKey(
                            IntSerializer.INSTANCE, inputView, false);
            assertThat(deserializedKey).isEqualTo(orgKey);

            CompositeKeySerializationUtils.writeKey(
                    orgKey, IntSerializer.INSTANCE, outputView, true);
            inputView.setBuffer(outputView.getCopyOfBuffer());
            deserializedKey =
                    CompositeKeySerializationUtils.readKey(IntSerializer.INSTANCE, inputView, true);
            assertThat(deserializedKey).isEqualTo(orgKey);
        }
    }

    @Test
    void testNamespaceSerializationAndDeserialization() throws Exception {
        final DataOutputSerializer outputView = new DataOutputSerializer(8);
        final DataInputDeserializer inputView = new DataInputDeserializer();

        for (int orgNamespace = 0; orgNamespace < 100; ++orgNamespace) {
            outputView.clear();
            CompositeKeySerializationUtils.writeNameSpace(
                    orgNamespace, IntSerializer.INSTANCE, outputView, false);
            inputView.setBuffer(outputView.getCopyOfBuffer());
            int deserializedNamepsace =
                    CompositeKeySerializationUtils.readNamespace(
                            IntSerializer.INSTANCE, inputView, false);
            assertThat(deserializedNamepsace).isEqualTo(orgNamespace);

            CompositeKeySerializationUtils.writeNameSpace(
                    orgNamespace, IntSerializer.INSTANCE, outputView, true);
            inputView.setBuffer(outputView.getCopyOfBuffer());
            deserializedNamepsace =
                    CompositeKeySerializationUtils.readNamespace(
                            IntSerializer.INSTANCE, inputView, true);
            assertThat(deserializedNamepsace).isEqualTo(orgNamespace);
        }
    }
}
