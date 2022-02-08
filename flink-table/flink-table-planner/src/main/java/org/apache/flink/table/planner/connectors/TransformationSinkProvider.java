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

package org.apache.flink.table.planner.connectors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.sink.SinkProvider;
import org.apache.flink.table.data.RowData;

import java.util.Optional;

/**
 * Provider that produces a {@link Transformation} as a runtime implementation for {@link
 * DynamicTableSink}.
 *
 * <p>Note: This provider is only meant for advanced connector developers. Usually, a sink should
 * consist of a single entity expressed via {@link OutputFormatProvider}, {@link
 * SinkFunctionProvider}, or {@link SinkProvider}.
 */
@Internal
public interface TransformationSinkProvider extends DynamicTableSink.SinkRuntimeProvider {

    /**
     * Creates a transformation for transforming the input provided in the context.
     *
     * <p>This method MUST set an uid for each node of the transformation sink, when the job is
     * unbounded, which can be generated with {@link Context#generateUid(String)}.
     */
    Transformation<?> createTransformation(Context context);

    /** Context for {@link #createTransformation(Context)}. */
    interface Context extends ProviderContext {

        /** Helper method for creating the default implementation of {@link Context}. */
        static Context of(
                Transformation<RowData> inputTransformation,
                int rowtimeIndex,
                ProviderContext providerContext) {
            return new Context() {

                @Override
                public Transformation<RowData> getInputTransformation() {
                    return inputTransformation;
                }

                @Override
                public int getRowtimeIndex() {
                    return rowtimeIndex;
                }

                @Override
                public Optional<String> generateUid(String name) {
                    return providerContext.generateUid(name);
                }
            };
        }

        /** Input transformation to transform. */
        Transformation<RowData> getInputTransformation();

        /** Returns the field index of the rowtime attribute, or {@code -1} otherwise. */
        int getRowtimeIndex();
    }
}
