/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeinfo.IntegerTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.runtime.operators.sink.TestSink;

import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.core.memory.ManagedMemoryUseCase.STATE_BACKEND;

/** Unit tests for {@link SinkTransformation}. */
public class SinkTransformationTest {

    @Test(expected = UnsupportedOperationException.class)
    public void unSupportSetResource() {
        final SinkTransformation<Integer, String, String, String> sinkTransformation =
                createSinkTransformation();

        sinkTransformation.setResources(
                ResourceSpec.newBuilder(1, 1).build(), ResourceSpec.newBuilder(2, 2).build());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void unSupportDeclareOperatorScopeManagedMemory() {
        final SinkTransformation<Integer, String, String, String> sinkTransformation =
                createSinkTransformation();
        sinkTransformation.declareManagedMemoryUseCaseAtOperatorScope(STATE_BACKEND, 1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void unSupportDeclareSlotScopeManagedMemory() {
        final SinkTransformation<Integer, String, String, String> sinkTransformation =
                createSinkTransformation();
        sinkTransformation.declareManagedMemoryUseCaseAtSlotScope(STATE_BACKEND);
    }

    private static SinkTransformation<Integer, String, String, String> createSinkTransformation() {
        return new SinkTransformation<>(
                new TestTransformation<>("src", IntegerTypeInfo.INT_TYPE_INFO, 1),
                TestSink.newBuilder().build(),
                "sink",
                1);
    }

    private static class TestTransformation<T> extends Transformation<T> {

        public TestTransformation(String name, TypeInformation<T> outputType, int parallelism) {
            super(name, outputType, parallelism);
        }

        @Override
        public List<Transformation<?>> getTransitivePredecessors() {
            return Collections.emptyList();
        }

        @Override
        public List<Transformation<?>> getInputs() {
            return Collections.emptyList();
        }
    }
}
