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

import org.apache.flink.api.common.serialization.SerializationSchema;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TProtocolFactory;

/**
 * Serialization schema that serializes to THRIFT object binary format.
 *
 * @param <T> the type to be serialized
 */
public class ThriftSerializationSchema<T extends TBase> implements SerializationSchema<T> {

    private Class<? extends TProtocolFactory> tPClass;

    private transient TSerializer tSerializer;

    public ThriftSerializationSchema(Class<? extends TProtocolFactory> tPClass) {
        this.tPClass = tPClass;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        tSerializer = new TSerializer(tPClass.newInstance());
    }

    @Override
    public byte[] serialize(T element) {
        try {
            return tSerializer.serialize(element);
        } catch (TException e) {
            throw new RuntimeException("Serialize thrift object error.", e);
        }
    }
}
