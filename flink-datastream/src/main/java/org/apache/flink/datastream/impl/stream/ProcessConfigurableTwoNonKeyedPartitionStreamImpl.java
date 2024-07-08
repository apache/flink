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
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream.ProcessConfigurableAndTwoNonKeyedPartitionStream;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream;
import org.apache.flink.datastream.impl.ExecutionEnvironmentImpl;
import org.apache.flink.datastream.impl.utils.StreamUtils;

/**
 * {@link ProcessConfigurableTwoNonKeyedPartitionStreamImpl} is used to hold the two output
 * non-keyed streams and provide methods used for configuration.
 */
public class ProcessConfigurableTwoNonKeyedPartitionStreamImpl<T, OUT1, OUT2>
        extends ProcessConfigureHandle<
                T, ProcessConfigurableAndTwoNonKeyedPartitionStream<T, OUT1, OUT2>>
        implements ProcessConfigurableAndTwoNonKeyedPartitionStream<T, OUT1, OUT2> {

    private final NonKeyedPartitionStreamImpl<OUT1> firstStream;

    private final NonKeyedPartitionStreamImpl<OUT2> secondStream;

    public ProcessConfigurableTwoNonKeyedPartitionStreamImpl(
            ExecutionEnvironmentImpl environment,
            Transformation<T> transformation,
            NonKeyedPartitionStreamImpl<OUT1> firstStream,
            NonKeyedPartitionStreamImpl<OUT2> secondStream) {
        super(environment, transformation);
        this.firstStream = firstStream;
        this.secondStream = secondStream;
    }

    @Override
    public ProcessConfigurableAndNonKeyedPartitionStream<OUT1> getFirst() {
        return StreamUtils.wrapWithConfigureHandle(firstStream);
    }

    @Override
    public ProcessConfigurableAndNonKeyedPartitionStream<OUT2> getSecond() {
        return StreamUtils.wrapWithConfigureHandle(secondStream);
    }
}
