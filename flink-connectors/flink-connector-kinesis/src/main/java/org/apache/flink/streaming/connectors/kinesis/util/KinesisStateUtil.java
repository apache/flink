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

package org.apache.flink.streaming.connectors.kinesis.util;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardMetadata;

/** Utilities for Flink Kinesis connector state management. */
public class KinesisStateUtil {

    /** To prevent instantiation of class. */
    private KinesisStateUtil() {}

    /**
     * Creates state serializer for kinesis shard sequence number. Using of the explicit state
     * serializer with KryoSerializer is needed because otherwise users cannot use
     * 'disableGenericTypes' properties with KinesisConsumer, see FLINK-24943 for details
     *
     * @return state serializer
     */
    public static TupleSerializer<Tuple2<StreamShardMetadata, SequenceNumber>>
            createShardsStateSerializer(ExecutionConfig executionConfig) {
        // explicit serializer will keep the compatibility with GenericTypeInformation
        // and allow to disableGenericTypes for users
        TypeSerializer<?>[] fieldSerializers =
                new TypeSerializer<?>[] {
                    TypeInformation.of(StreamShardMetadata.class).createSerializer(executionConfig),
                    new KryoSerializer<>(SequenceNumber.class, executionConfig)
                };
        @SuppressWarnings("unchecked")
        Class<Tuple2<StreamShardMetadata, SequenceNumber>> tupleClass =
                (Class<Tuple2<StreamShardMetadata, SequenceNumber>>) (Class<?>) Tuple2.class;
        return new TupleSerializer<>(tupleClass, fieldSerializers);
    }
}
