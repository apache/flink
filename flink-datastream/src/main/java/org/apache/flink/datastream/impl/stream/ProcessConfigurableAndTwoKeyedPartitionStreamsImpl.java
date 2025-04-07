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

package org.apache.flink.datastream.impl.stream;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream.ProcessConfigurableAndKeyedPartitionStream;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream.ProcessConfigurableAndTwoKeyedPartitionStreams;
import org.apache.flink.datastream.impl.ExecutionEnvironmentImpl;
import org.apache.flink.datastream.impl.utils.StreamUtils;

/**
 * {@link ProcessConfigurableAndTwoKeyedPartitionStreamsImpl} is used to hold the two output keyed
 * streams and provide methods used for configuration.
 */
public class ProcessConfigurableAndTwoKeyedPartitionStreamsImpl<K, OUT1, OUT2>
        extends ProcessConfigureHandle<
                OUT1, ProcessConfigurableAndTwoKeyedPartitionStreams<K, OUT1, OUT2>>
        implements ProcessConfigurableAndTwoKeyedPartitionStreams<K, OUT1, OUT2> {
    private final KeyedPartitionStreamImpl<K, OUT1> firstStream;

    private final KeyedPartitionStreamImpl<K, OUT2> secondStream;

    public ProcessConfigurableAndTwoKeyedPartitionStreamsImpl(
            ExecutionEnvironmentImpl environment,
            Transformation<OUT1> transformation,
            KeyedPartitionStreamImpl<K, OUT1> firstStream,
            KeyedPartitionStreamImpl<K, OUT2> secondStream) {
        super(environment, transformation);
        this.firstStream = firstStream;
        this.secondStream = secondStream;
    }

    @Override
    public ProcessConfigurableAndKeyedPartitionStream<K, OUT1> getFirst() {
        return StreamUtils.wrapWithConfigureHandle(firstStream);
    }

    @Override
    public ProcessConfigurableAndKeyedPartitionStream<K, OUT2> getSecond() {
        return StreamUtils.wrapWithConfigureHandle(secondStream);
    }
}
