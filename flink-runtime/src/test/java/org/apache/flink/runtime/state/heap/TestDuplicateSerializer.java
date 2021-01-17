/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import org.junit.Assert;

import java.io.IOException;

/**
 * This serializer can be used to test serializer duplication. The serializer that can be disabled.
 * Duplicates are still enabled, so we can check that serializers are duplicated.
 */
public class TestDuplicateSerializer extends TypeSerializer<Integer> {

    private static final long serialVersionUID = 1L;

    private static final Integer ZERO = 0;

    private boolean disabled;

    public TestDuplicateSerializer() {
        this.disabled = false;
    }

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public TypeSerializer<Integer> duplicate() {
        return new TestDuplicateSerializer();
    }

    @Override
    public Integer createInstance() {
        return ZERO;
    }

    @Override
    public Integer copy(Integer from) {
        return from;
    }

    @Override
    public Integer copy(Integer from, Integer reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return 4;
    }

    @Override
    public void serialize(Integer record, DataOutputView target) throws IOException {
        Assert.assertFalse(disabled);
        target.writeInt(record);
    }

    @Override
    public Integer deserialize(DataInputView source) throws IOException {
        Assert.assertFalse(disabled);
        return source.readInt();
    }

    @Override
    public Integer deserialize(Integer reuse, DataInputView source) throws IOException {
        Assert.assertFalse(disabled);
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        Assert.assertFalse(disabled);
        target.writeInt(source.readInt());
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof TestDuplicateSerializer;
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    public void disable() {
        this.disabled = true;
    }

    @Override
    public TypeSerializerSnapshot<Integer> snapshotConfiguration() {
        throw new UnsupportedOperationException();
    }
}
