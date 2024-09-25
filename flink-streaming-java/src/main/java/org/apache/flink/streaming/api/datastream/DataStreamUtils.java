/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;

import java.util.Iterator;

/** A collection of utilities for {@link DataStream DataStreams}. */
@Experimental
public final class DataStreamUtils {

    /**
     * Triggers the distributed execution of the streaming dataflow and returns an iterator over the
     * elements of the given DataStream.
     *
     * <p>The DataStream application is executed in the regular distributed manner on the target
     * environment, and the events from the stream are polled back to this application process and
     * thread through Flink's REST API.
     *
     * @deprecated Please use {@link DataStream#executeAndCollect()}.
     */
    @Deprecated
    public static <OUT> Iterator<OUT> collect(DataStream<OUT> stream) {
        return collect(stream, "Data Stream Collect");
    }

    /**
     * Triggers the distributed execution of the streaming dataflow and returns an iterator over the
     * elements of the given DataStream.
     *
     * <p>The DataStream application is executed in the regular distributed manner on the target
     * environment, and the events from the stream are polled back to this application process and
     * thread through Flink's REST API.
     *
     * @deprecated Please use {@link DataStream#executeAndCollect()}.
     */
    @Deprecated
    public static <OUT> Iterator<OUT> collect(DataStream<OUT> stream, String executionJobName) {
        try {
            return stream.executeAndCollect(executionJobName);
        } catch (Exception e) {
            // this "wrap as unchecked" step is here only to preserve the exception signature
            // backwards compatible.
            throw new RuntimeException("Failed to execute data stream", e);
        }
    }

    // ------------------------------------------------------------------------
    //  Deriving a KeyedStream from a stream already partitioned by key
    //  without a shuffle
    // ------------------------------------------------------------------------

    /**
     * Reinterprets the given {@link DataStream} as a {@link KeyedStream}, which extracts keys with
     * the given {@link KeySelector}.
     *
     * <p>IMPORTANT: For every partition of the base stream, the keys of events in the base stream
     * must be partitioned exactly in the same way as if it was created through a {@link
     * DataStream#keyBy(KeySelector)}.
     *
     * @param stream The data stream to reinterpret. For every partition, this stream must be
     *     partitioned exactly in the same way as if it was created through a {@link
     *     DataStream#keyBy(KeySelector)}.
     * @param keySelector Function that defines how keys are extracted from the data stream.
     * @param <T> Type of events in the data stream.
     * @param <K> Type of the extracted keys.
     * @return The reinterpretation of the {@link DataStream} as a {@link KeyedStream}.
     */
    public static <T, K> KeyedStream<T, K> reinterpretAsKeyedStream(
            DataStream<T> stream, KeySelector<T, K> keySelector) {

        return reinterpretAsKeyedStream(
                stream,
                keySelector,
                TypeExtractor.getKeySelectorTypes(keySelector, stream.getType()));
    }

    /**
     * Reinterprets the given {@link DataStream} as a {@link KeyedStream}, which extracts keys with
     * the given {@link KeySelector}.
     *
     * <p>IMPORTANT: For every partition of the base stream, the keys of events in the base stream
     * must be partitioned exactly in the same way as if it was created through a {@link
     * DataStream#keyBy(KeySelector)}.
     *
     * @param stream The data stream to reinterpret. For every partition, this stream must be
     *     partitioned exactly in the same way as if it was created through a {@link
     *     DataStream#keyBy(KeySelector)}.
     * @param keySelector Function that defines how keys are extracted from the data stream.
     * @param typeInfo Explicit type information about the key type.
     * @param <T> Type of events in the data stream.
     * @param <K> Type of the extracted keys.
     * @return The reinterpretation of the {@link DataStream} as a {@link KeyedStream}.
     */
    public static <T, K> KeyedStream<T, K> reinterpretAsKeyedStream(
            DataStream<T> stream, KeySelector<T, K> keySelector, TypeInformation<K> typeInfo) {

        PartitionTransformation<T> partitionTransformation =
                new PartitionTransformation<>(
                        stream.getTransformation(), new ForwardPartitioner<>());

        return new KeyedStream<>(stream, partitionTransformation, keySelector, typeInfo);
    }

    // ------------------------------------------------------------------------

    /** Private constructor to prevent instantiation. */
    private DataStreamUtils() {}

    // ------------------------------------------------------------------------

}
