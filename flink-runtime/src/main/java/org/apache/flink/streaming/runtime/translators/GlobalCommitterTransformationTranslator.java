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

package org.apache.flink.streaming.runtime.translators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.GlobalCommitterOperator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.GlobalCommitterTransform;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;
import org.apache.flink.streaming.runtime.operators.sink.CommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperatorFactory;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

import static org.apache.flink.streaming.api.connector.sink2.StandardSinkTopologies.GLOBAL_COMMITTER_TRANSFORMATION_NAME;

/**
 * A {@link TransformationTranslator} for the {@link GlobalCommitterOperator}. The main purpose is
 * to detect whether we set {@link GlobalCommitterOperator#commitOnInput} or not.
 */
@Internal
public class GlobalCommitterTransformationTranslator<CommT>
        implements TransformationTranslator<Void, GlobalCommitterTransform<CommT>> {

    @Override
    public Collection<Integer> translateForBatch(
            GlobalCommitterTransform<CommT> transformation, Context context) {
        return translateInternal(transformation, true);
    }

    @Override
    public Collection<Integer> translateForStreaming(
            GlobalCommitterTransform<CommT> transformation, Context context) {
        return translateInternal(transformation, false);
    }

    private Collection<Integer> translateInternal(
            GlobalCommitterTransform<CommT> globalCommitterTransform, boolean batch) {
        DataStream<CommittableMessage<CommT>> inputStream =
                globalCommitterTransform.getInputStream();
        boolean checkpointingEnabled =
                inputStream
                        .getExecutionEnvironment()
                        .getCheckpointConfig()
                        .isCheckpointingEnabled();
        boolean commitOnInput = batch || !checkpointingEnabled || hasUpstreamCommitter(inputStream);

        // Create a global shuffle and add the global committer with parallelism 1.
        final PhysicalTransformation<Void> transformation =
                (PhysicalTransformation<Void>)
                        inputStream
                                .global()
                                .transform(
                                        GLOBAL_COMMITTER_TRANSFORMATION_NAME,
                                        Types.VOID,
                                        new GlobalCommitterOperator<>(
                                                globalCommitterTransform.getCommitterFactory(),
                                                globalCommitterTransform.getCommittableSerializer(),
                                                commitOnInput))
                                .getTransformation();
        transformation.setChainingStrategy(ChainingStrategy.ALWAYS);
        transformation.setName(GLOBAL_COMMITTER_TRANSFORMATION_NAME);
        transformation.setParallelism(1);
        transformation.setMaxParallelism(1);
        return Collections.emptyList();
    }

    /**
     * Looks for a committer in the pipeline and aborts on writer. The GlobalCommitter behaves
     * differently if there is a committer after the writer.
     */
    private static boolean hasUpstreamCommitter(DataStream<?> ds) {
        Transformation<?> dsTransformation = ds.getTransformation();

        Set<Integer> seenIds = new HashSet<>();
        Queue<Transformation<?>> pendingsTransformations =
                new ArrayDeque<>(Collections.singleton(dsTransformation));
        while (!pendingsTransformations.isEmpty()) {
            Transformation<?> transformation = pendingsTransformations.poll();
            if (transformation instanceof OneInputTransformation) {
                StreamOperatorFactory<?> operatorFactory =
                        ((OneInputTransformation<?, ?>) transformation).getOperatorFactory();
                if (operatorFactory instanceof CommitterOperatorFactory) {
                    return true;
                }
                if (operatorFactory instanceof SinkWriterOperatorFactory) {
                    // don't look at the inputs of the writer
                    continue;
                }
            }
            for (Transformation<?> input : transformation.getInputs()) {
                if (seenIds.add(input.getId())) {
                    pendingsTransformations.add(input);
                }
            }
        }

        return false;
    }
}
