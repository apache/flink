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

package org.apache.flink.streaming.util;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Utilities for {@link SourceFunction}. */
public class SourceFunctionUtil {

    public static <T extends Serializable> List<T> runSourceFunction(
            SourceFunction<T> sourceFunction) throws Exception {
        if (sourceFunction instanceof RichFunction) {
            return runRichSourceFunction(sourceFunction);
        } else {
            return runNonRichSourceFunction(sourceFunction);
        }
    }

    private static <T extends Serializable> List<T> runRichSourceFunction(
            SourceFunction<T> sourceFunction) throws Exception {
        try (MockEnvironment environment =
                new MockEnvironmentBuilder()
                        .setTaskName("MockTask")
                        .setManagedMemorySize(3 * 1024 * 1024)
                        .setInputSplitProvider(new MockInputSplitProvider())
                        .setBufferSize(1024)
                        .build()) {

            AbstractStreamOperator<?> operator = mock(AbstractStreamOperator.class);
            when(operator.getExecutionConfig()).thenReturn(new ExecutionConfig());

            RuntimeContext runtimeContext =
                    new StreamingRuntimeContext(operator, environment, new HashMap<>());
            ((RichFunction) sourceFunction).setRuntimeContext(runtimeContext);
            ((RichFunction) sourceFunction).open(new Configuration());

            return runNonRichSourceFunction(sourceFunction);
        }
    }

    private static <T extends Serializable> List<T> runNonRichSourceFunction(
            SourceFunction<T> sourceFunction) {
        final List<T> outputs = new ArrayList<>();
        try {
            SourceFunction.SourceContext<T> ctx =
                    new CollectingSourceContext<T>(new Object(), outputs);
            sourceFunction.run(ctx);
        } catch (Exception e) {
            throw new RuntimeException("Cannot invoke source.", e);
        }
        return outputs;
    }
}
