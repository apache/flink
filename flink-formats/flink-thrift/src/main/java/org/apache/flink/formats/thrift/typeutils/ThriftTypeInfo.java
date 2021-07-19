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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;

import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TProtocolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftInfoInfo is for creating TypeInformation for Thrift classes.
 *
 * @param <T> The Thrift class
 */
public class ThriftTypeInfo<T extends TBase> extends GenericTypeInfo<T> {
    private static final Logger LOG = LoggerFactory.getLogger(ThriftTypeInfo.class);

    public Class<T> tClass;
    public Class<? extends TProtocolFactory> tPClass;

    public ThriftTypeInfo(Class<T> tClass, Class<? extends TProtocolFactory> tPClass) {
        super(tClass);
        this.tClass = tClass;
        this.tPClass = tPClass;
    }

    @PublicEvolving
    public TypeSerializer<T> createSerializer(ExecutionConfig executionConfig) {
        return new ThriftSerializer<>(tClass, tPClass);
    }
}
