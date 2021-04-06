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
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.operators.collect.ClientAndIterator;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

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

    /**
     * Starts the execution of the program and returns an iterator to read the result of the given
     * data stream, plus a {@link JobClient} to interact with the application execution.
     *
     * @deprecated Please use {@link DataStream#executeAndCollect()}.
     */
    @Deprecated
    public static <OUT> ClientAndIterator<OUT> collectWithClient(
            DataStream<OUT> stream, String jobExecutionName) throws Exception {
        return stream.executeAndCollectWithClient(jobExecutionName);
    }

    /**
     * Collects contents the given DataStream into a list, assuming that the stream is a bounded
     * stream.
     *
     * <p>This method blocks until the job execution is complete. By the time the method returns,
     * the job will have reached its FINISHED status.
     *
     * <p>Note that if the stream is unbounded, this method will never return and might fail with an
     * Out-of-Memory Error because it attempts to collect an infinite stream into a list.
     *
     * @throws Exception Exceptions that occur during the execution are forwarded.
     * @deprecated Please use {@link DataStream#executeAndCollect()}.
     */
    @Deprecated
    public static <E> List<E> collectBoundedStream(DataStream<E> stream, String jobName)
            throws Exception {
        final ArrayList<E> list = new ArrayList<>();
        final Iterator<E> iter = collectWithClient(stream, jobName).iterator;
        while (iter.hasNext()) {
            list.add(iter.next());
        }
        list.trimToSize();
        return list;
    }

    /**
     * Triggers execution of the DataStream application and collects the given number of records
     * from the stream. After the records are received, the execution is canceled.
     *
     * @deprecated Please use {@link DataStream#executeAndCollect()}.
     */
    @Deprecated
    public static <E> List<E> collectUnboundedStream(
            DataStream<E> stream, int numElements, String jobName) throws Exception {
        final ClientAndIterator<E> clientAndIterator = collectWithClient(stream, jobName);
        final List<E> result = collectRecordsFromUnboundedStream(clientAndIterator, numElements);

        // cancel the job not that we have received enough elements
        clientAndIterator.client.cancel().get();

        return result;
    }

    /** @deprecated Please use {@link DataStream#executeAndCollect()}. */
    @Deprecated
    public static <E> List<E> collectRecordsFromUnboundedStream(
            final ClientAndIterator<E> client, final int numElements) {

        checkNotNull(client, "client");
        checkArgument(numElements > 0, "numElement must be > 0");

        final ArrayList<E> result = new ArrayList<>(numElements);
        final Iterator<E> iterator = client.iterator;

        while (iterator.hasNext()) {
            result.add(iterator.next());
            if (result.size() == numElements) {
                return result;
            }
        }

        throw new IllegalArgumentException(
                String.format(
                        "The stream ended before reaching the requested %d records. Only %d records were received.",
                        numElements, result.size()));
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
