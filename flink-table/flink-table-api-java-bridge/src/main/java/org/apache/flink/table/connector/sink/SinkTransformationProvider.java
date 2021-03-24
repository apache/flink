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

package org.apache.flink.table.connector.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

/**
 * Transformation provider for sinks.
 *
 * <p>This is only used internally for {@link
 * org.apache.flink.table.api.bridge.java.StreamTableEnvironment#toDataStream}.
 */
@Internal
public interface SinkTransformationProvider extends DynamicTableSink.SinkRuntimeProvider {

    /** Transform the {@link Transformation} provided in the context. */
    Transformation<Object> getTransformation(Context context);

    /** Context for {@link #getTransformation}. */
    interface Context {
        /** Input transformation to transform. */
        Transformation<RowData> getInputTransformation();

        /** Returns the field index of the rowtime attribute, or {@code -1} otherwise. */
        int getRowtimeIndex();

        /**
         * Whether {@link org.apache.flink.configuration.PipelineOptions#OBJECT_REUSE} is enabled.
         */
        boolean isObjectReuseEnabled();
    }

    /** Default implementation of {@link Context}. */
    class DefaultContext implements Context {
        private final Transformation<RowData> inputTransformation;
        private final int rowtimeIndex;
        private final boolean objectReuseEnabled;

        public DefaultContext(
                Transformation<RowData> inputTransformation,
                int rowtimeIndex,
                boolean objectReuseEnabled) {
            Preconditions.checkNotNull(inputTransformation);

            this.inputTransformation = inputTransformation;
            this.rowtimeIndex = rowtimeIndex;
            this.objectReuseEnabled = objectReuseEnabled;
        }

        @Override
        public Transformation<RowData> getInputTransformation() {
            return inputTransformation;
        }

        @Override
        public int getRowtimeIndex() {
            return rowtimeIndex;
        }

        @Override
        public boolean isObjectReuseEnabled() {
            return objectReuseEnabled;
        }
    }
}
