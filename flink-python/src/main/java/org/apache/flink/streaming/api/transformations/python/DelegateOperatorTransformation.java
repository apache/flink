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

package org.apache.flink.streaming.api.transformations.python;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.python.DataStreamPythonFunctionOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * For those {@link org.apache.flink.api.dag.Transformation} that don't have an operator entity,
 * {@link DelegateOperatorTransformation} provides a {@link SimpleOperatorFactory} containing a
 * {@link DelegateOperator} , which can hold {@link OutputTag}s when optimizing transformations for
 * Python jobs, and later be queried at the translation stage.
 */
public interface DelegateOperatorTransformation<OUT> {

    SimpleOperatorFactory<OUT> getOperatorFactory();

    /** {@link DelegateOperator} holds {@link OutputTag}s. */
    class DelegateOperator<OUT> implements DataStreamPythonFunctionOperator<OUT> {

        private final Map<String, OutputTag<?>> sideOutputTags = new HashMap<>();

        @Override
        public void addSideOutputTags(Collection<OutputTag<?>> outputTags) {
            for (OutputTag<?> outputTag : outputTags) {
                sideOutputTags.put(outputTag.getId(), outputTag);
            }
        }

        @Override
        public Collection<OutputTag<?>> getSideOutputTags() {
            return sideOutputTags.values();
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {}

        @Override
        public TypeInformation<OUT> getProducedType() {
            return null;
        }

        @Override
        public void setNumPartitions(int numPartitions) {}

        @Override
        public DataStreamPythonFunctionInfo getPythonFunctionInfo() {
            return null;
        }

        @Override
        public <T> DataStreamPythonFunctionOperator<T> copy(
                DataStreamPythonFunctionInfo pythonFunctionInfo,
                TypeInformation<T> outputTypeInfo) {
            return null;
        }

        @Override
        public void setCurrentKey(Object key) {}

        @Override
        public Object getCurrentKey() {
            return null;
        }

        @Override
        public void open() throws Exception {}

        @Override
        public void finish() throws Exception {}

        @Override
        public void close() throws Exception {}

        @Override
        public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {}

        @Override
        public OperatorSnapshotFutures snapshotState(
                long checkpointId,
                long timestamp,
                CheckpointOptions checkpointOptions,
                CheckpointStreamFactory storageLocation)
                throws Exception {
            return null;
        }

        @Override
        public void initializeState(StreamTaskStateInitializer streamTaskStateManager)
                throws Exception {}

        @Override
        public void setKeyContextElement1(StreamRecord<?> record) throws Exception {}

        @Override
        public void setKeyContextElement2(StreamRecord<?> record) throws Exception {}

        @Override
        public OperatorMetricGroup getMetricGroup() {
            return null;
        }

        @Override
        public OperatorID getOperatorID() {
            return null;
        }
    }
}
