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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.python.AbstractPythonFunctionOperator;
import org.apache.flink.streaming.api.operators.python.DataStreamPythonFunctionOperator;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * For those {@link org.apache.flink.api.dag.Transformation} that don't have an operator entity,
 * {@link DelegateOperatorTransformation} provides a {@link SimpleOperatorFactory} containing a
 * {@link DelegateOperator} , which can hold special configurations during transformation
 * preprocessing for Python jobs, and later be queried at translation stage. Currently, those
 * configurations include {@link OutputTag}s, {@code numPartitions} and general {@link
 * Configuration}.
 */
public interface DelegateOperatorTransformation<OUT> {

    SimpleOperatorFactory<OUT> getOperatorFactory();

    static void configureOperator(
            DelegateOperatorTransformation<?> transformation,
            AbstractPythonFunctionOperator<?> operator) {
        DelegateOperator<?> delegateOperator =
                (DelegateOperator<?>) transformation.getOperatorFactory().getOperator();

        operator.getConfiguration().addAll(delegateOperator.getConfiguration());

        if (operator instanceof DataStreamPythonFunctionOperator) {
            DataStreamPythonFunctionOperator<?> dataStreamOperator =
                    (DataStreamPythonFunctionOperator<?>) operator;
            dataStreamOperator.addSideOutputTags(delegateOperator.getSideOutputTags());
            if (delegateOperator.getNumPartitions() != null) {
                dataStreamOperator.setNumPartitions(delegateOperator.getNumPartitions());
            }
        }
    }

    /**
     * {@link DelegateOperator} holds configurations, e.g. {@link OutputTag}s, which will be applied
     * to the actual python operator at translation stage.
     */
    class DelegateOperator<OUT> extends AbstractPythonFunctionOperator<OUT>
            implements DataStreamPythonFunctionOperator<OUT> {

        private final Map<String, OutputTag<?>> sideOutputTags = new HashMap<>();
        private @Nullable Integer numPartitions = null;

        public DelegateOperator() {
            super(new Configuration());
        }

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
        public void setNumPartitions(int numPartitions) {
            this.numPartitions = numPartitions;
        }

        @Nullable
        public Integer getNumPartitions() {
            return numPartitions;
        }

        @Override
        public TypeInformation<OUT> getProducedType() {
            throw new RuntimeException("This should not be invoked on a DelegateOperator!");
        }

        @Override
        public DataStreamPythonFunctionInfo getPythonFunctionInfo() {
            throw new RuntimeException("This should not be invoked on a DelegateOperator!");
        }

        @Override
        public <T> DataStreamPythonFunctionOperator<T> copy(
                DataStreamPythonFunctionInfo pythonFunctionInfo,
                TypeInformation<T> outputTypeInfo) {
            throw new RuntimeException("This should not be invoked on a DelegateOperator!");
        }

        @Override
        protected void invokeFinishBundle() throws Exception {
            throw new RuntimeException("This should not be invoked on a DelegateOperator!");
        }

        @Override
        protected PythonEnvironmentManager createPythonEnvironmentManager() {
            throw new RuntimeException("This should not be invoked on a DelegateOperator!");
        }
    }
}
