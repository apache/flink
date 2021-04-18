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

package org.apache.flink.formats.thrift.typeutils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TProtocolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** A serializer that serializes types via Thrift. */
public class ThriftSerializer<T extends TBase> extends TypeSerializer<T> {
    private static final Logger LOG = LoggerFactory.getLogger(ThriftSerializer.class);

    public Class<T> tClass;
    public Class<? extends TProtocolFactory> tPClass;
    private ThriftSerializerSnapshot configSnapshot;
    private transient TSerializer tSerializer;
    private transient TDeserializer tDeserializer;
    private transient T reuse;

    public ThriftSerializer(Class<T> tClass, Class<? extends TProtocolFactory> tPClass) {
        this.tClass = tClass;
        this.tPClass = tPClass;
    }

    // ------------------------------------------------------------------------
    //  Compatibility and Upgrades
    // ------------------------------------------------------------------------

    @Override
    public TypeSerializerSnapshot<T> snapshotConfiguration() {
        if (configSnapshot == null) {
            configSnapshot = new ThriftSerializerSnapshot<>(tClass, tPClass);
        }
        return configSnapshot;
    }

    @Override
    public T deserialize(DataInputView source) throws IOException {
        int length = source.readInt();
        byte[] arr = new byte[length];
        source.read(arr);
        try {
            tDeserializer.deserialize(reuse, arr);
            return reuse;
        } catch (TException e) {
            throw new IOException(e);
        }
    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        int length = source.readInt();
        byte[] arr = new byte[length];
        source.read(arr);
        try {
            tDeserializer.deserialize(reuse, arr);
            return reuse;
        } catch (TException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int length = source.readInt();
        byte[] arr = new byte[length];
        source.read(arr);
        target.writeInt(length);
        target.write(arr);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (object == null || object.getClass() != this.getClass()) {
            return false;
        }

        ThriftSerializer that = (ThriftSerializer) object;
        return this.tClass.equals(that.tClass) && this.tPClass.equals(that.tPClass);
    }

    @Override
    public int hashCode() {
        int result = tClass.hashCode();
        result += 31 * result + tPClass.hashCode();
        return result;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<T> duplicate() {
        return new ThriftSerializer<>(tClass, tPClass);
    }

    @Override
    public T createInstance() {
        try {
            return tClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("create instance error", e);
        }
    }

    @Override
    public T copy(T from) {
        return (T) from.deepCopy();
    }

    @Override
    public T copy(T from, T reuse) {
        return (T) from.deepCopy();
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(T record, DataOutputView target) throws IOException {
        checkThriftInitialized();
        try {
            byte[] arr = tSerializer.serialize(record);
            target.writeInt(arr.length);
            target.write(arr);
        } catch (TException e) {
            throw new IOException(e);
        }
    }

    void checkThriftInitialized() {
        try {
            if (reuse == null) {
                reuse = tClass.newInstance();
            }
            if (tSerializer == null || tDeserializer == null) {
                TProtocolFactory tProtocolFactory = tPClass.newInstance();
                tSerializer = new TSerializer(tProtocolFactory);
                tDeserializer = new TDeserializer(tProtocolFactory);
            }
        } catch (Exception e) {
            throw new RuntimeException("Thrift protocol factory class initialized error");
        }
    }
}
