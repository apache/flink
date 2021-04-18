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

package org.apache.flink.formats.thrift;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.thrift.typeutils.ThriftTypeInfo;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TProtocolFactory;

import java.io.IOException;

/**
 * Deserialization schema that deserializes from Thrift object binary format.
 *
 * @param <T> type of record it produces
 */
public class ThriftDeserializationSchema<T extends TBase> implements DeserializationSchema<T> {

    private final Class<T> tClass;
    private final Class<? extends TProtocolFactory> tPClass;
    private transient TDeserializer tDeserializer;

    public ThriftDeserializationSchema(Class<T> tClass, Class<? extends TProtocolFactory> tPClass) {
        this.tClass = tClass;
        this.tPClass = tPClass;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        tDeserializer = new TDeserializer(tPClass.newInstance());
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        try {
            T t = tClass.newInstance();
            tDeserializer.deserialize(t, message);
            return t;
        } catch (Exception e) {
            throw new RuntimeException("Deserialize thrift object error", e);
        }
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return new ThriftTypeInfo(tClass, tPClass);
    }
}
