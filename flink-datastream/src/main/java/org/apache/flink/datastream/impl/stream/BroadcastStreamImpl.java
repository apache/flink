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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.datastream.api.function.TwoInputBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.stream.BroadcastStream;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream.ProcessConfigurableAndKeyedPartitionStream;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream;
import org.apache.flink.datastream.impl.ExecutionEnvironmentImpl;
import org.apache.flink.datastream.impl.operators.KeyedTwoInputBroadcastProcessOperator;
import org.apache.flink.datastream.impl.operators.TwoInputBroadcastProcessOperator;
import org.apache.flink.datastream.impl.utils.StreamUtils;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;

/** The implementation of {@link BroadcastStream}. */
public class BroadcastStreamImpl<T> extends AbstractDataStream<T> implements BroadcastStream<T> {
    public BroadcastStreamImpl(
            ExecutionEnvironmentImpl environment, Transformation<T> transformation) {
        this(
                environment,
                new PartitionTransformation<>(transformation, new BroadcastPartitioner<>()));
    }

    private BroadcastStreamImpl(
            ExecutionEnvironmentImpl environment, PartitionTransformation<T> transformation) {
        super(environment, transformation);
    }

    @Override
    public <K, T_OTHER, OUT> ProcessConfigurableAndNonKeyedPartitionStream<OUT> connectAndProcess(
            KeyedPartitionStream<K, T_OTHER> other,
            TwoInputBroadcastStreamProcessFunction<T_OTHER, T, OUT> processFunction) {
        TypeInformation<OUT> outTypeInfo =
                StreamUtils.getOutputTypeForTwoInputBroadcastProcessFunction(
                        processFunction,
                        ((KeyedPartitionStreamImpl<K, T_OTHER>) other).getType(),
                        getType());
        KeyedTwoInputBroadcastProcessOperator<K, T_OTHER, T, OUT> processOperator =
                new KeyedTwoInputBroadcastProcessOperator<>(processFunction);
        Transformation<OUT> outTransformation =
                StreamUtils.getTwoInputTransformation(
                        "Broadcast-Keyed-TwoInput-Process",
                        (KeyedPartitionStreamImpl<K, T_OTHER>) other,
                        // we should always take the broadcast input as second input.
                        this,
                        outTypeInfo,
                        processOperator);
        environment.addOperator(outTransformation);
        return StreamUtils.wrapWithConfigureHandle(
                new NonKeyedPartitionStreamImpl<>(environment, outTransformation));
    }

    @Override
    public <T_OTHER, OUT> ProcessConfigurableAndNonKeyedPartitionStream<OUT> connectAndProcess(
            NonKeyedPartitionStream<T_OTHER> other,
            TwoInputBroadcastStreamProcessFunction<T_OTHER, T, OUT> processFunction) {
        TypeInformation<OUT> outTypeInfo =
                StreamUtils.getOutputTypeForTwoInputBroadcastProcessFunction(
                        processFunction,
                        ((NonKeyedPartitionStreamImpl<T_OTHER>) other).getType(),
                        getType());
        TwoInputBroadcastProcessOperator<T_OTHER, T, OUT> processOperator =
                new TwoInputBroadcastProcessOperator<>(processFunction);
        Transformation<OUT> outTransformation =
                StreamUtils.getTwoInputTransformation(
                        "Broadcast-TwoInput-Process",
                        (NonKeyedPartitionStreamImpl<T_OTHER>) other,
                        // we should always take the broadcast input as second input.
                        this,
                        outTypeInfo,
                        processOperator);
        environment.addOperator(outTransformation);
        return StreamUtils.wrapWithConfigureHandle(
                new NonKeyedPartitionStreamImpl<>(environment, outTransformation));
    }

    @Override
    public <K, T_OTHER, OUT> ProcessConfigurableAndKeyedPartitionStream<K, OUT> connectAndProcess(
            KeyedPartitionStream<K, T_OTHER> other,
            TwoInputBroadcastStreamProcessFunction<T_OTHER, T, OUT> processFunction,
            KeySelector<OUT, K> newKeySelector) {
        TypeInformation<OUT> outTypeInfo =
                StreamUtils.getOutputTypeForTwoInputBroadcastProcessFunction(
                        processFunction,
                        ((KeyedPartitionStreamImpl<K, T_OTHER>) other).getType(),
                        getType());
        KeyedTwoInputBroadcastProcessOperator<K, T_OTHER, T, OUT> processOperator =
                new KeyedTwoInputBroadcastProcessOperator<>(processFunction);
        Transformation<OUT> outTransformation =
                StreamUtils.getTwoInputTransformation(
                        "Broadcast-Keyed-TwoInput-Process",
                        (KeyedPartitionStreamImpl<K, T_OTHER>) other,
                        // we should always take the broadcast input as second input.
                        this,
                        outTypeInfo,
                        processOperator);

        NonKeyedPartitionStreamImpl<OUT> outputStream =
                new NonKeyedPartitionStreamImpl<>(environment, outTransformation);
        environment.addOperator(outTransformation);
        // Construct a keyed stream directly without partitionTransformation to avoid shuffle.
        return StreamUtils.wrapWithConfigureHandle(
                new KeyedPartitionStreamImpl<>(
                        outputStream,
                        outTransformation,
                        newKeySelector,
                        TypeExtractor.getKeySelectorTypes(newKeySelector, outputStream.getType())));
    }
}
