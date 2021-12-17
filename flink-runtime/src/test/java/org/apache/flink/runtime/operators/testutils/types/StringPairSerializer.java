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

package org.apache.flink.runtime.operators.testutils.types;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.StringValue;

import java.io.IOException;

public class StringPairSerializer extends TypeSerializer<StringPair> {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public StringPairSerializer duplicate() {
        return this;
    }

    @Override
    public StringPair createInstance() {
        return new StringPair();
    }

    @Override
    public StringPair copy(StringPair from) {
        return new StringPair(from.getKey(), from.getValue());
    }

    @Override
    public StringPair copy(StringPair from, StringPair reuse) {
        reuse.setKey(from.getKey());
        reuse.setValue(from.getValue());
        return reuse;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(StringPair record, DataOutputView target) throws IOException {
        StringValue.writeString(record.getKey(), target);
        StringValue.writeString(record.getValue(), target);
    }

    @Override
    public StringPair deserialize(DataInputView source) throws IOException {
        return new StringPair(StringValue.readString(source), StringValue.readString(source));
    }

    @Override
    public StringPair deserialize(StringPair record, DataInputView source) throws IOException {
        record.setKey(StringValue.readString(source));
        record.setValue(StringValue.readString(source));
        return record;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        StringValue.writeString(StringValue.readString(source), target);
        StringValue.writeString(StringValue.readString(source), target);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof StringPairSerializer;
    }

    @Override
    public int hashCode() {
        return StringPairSerializer.class.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<StringPair> snapshotConfiguration() {
        throw new UnsupportedOperationException();
    }
}
