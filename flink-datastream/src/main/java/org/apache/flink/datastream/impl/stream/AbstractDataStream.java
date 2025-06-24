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
import org.apache.flink.datastream.api.stream.DataStream;
import org.apache.flink.datastream.impl.ExecutionEnvironmentImpl;
import org.apache.flink.streaming.api.transformations.SideOutputTransformation;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * Base class for all streams.
 *
 * <p>Note: This is only used for internal implementation. It must not leak to user face api.
 */
public abstract class AbstractDataStream<T> implements DataStream {
    protected final ExecutionEnvironmentImpl environment;

    protected final Transformation<T> transformation;

    /**
     * We keep track of the side outputs that were already requested and their types. With this, we
     * can catch the case when a side output with a matching id is requested for a different type
     * because this would lead to problems at runtime.
     */
    protected final Map<OutputTag<?>, TypeInformation<?>> requestedSideOutputs = new HashMap<>();

    public AbstractDataStream(
            ExecutionEnvironmentImpl environment, Transformation<T> transformation) {
        this.environment =
                Preconditions.checkNotNull(environment, "Execution Environment must not be null.");
        this.transformation =
                Preconditions.checkNotNull(
                        transformation, "Stream Transformation must not be null.");
    }

    public TypeInformation<T> getType() {
        return transformation.getOutputType();
    }

    /** This is only used for internal implementation. It must not leak to user face api. */
    public Transformation<T> getTransformation() {
        return transformation;
    }

    public ExecutionEnvironmentImpl getEnvironment() {
        return environment;
    }

    public <X> Transformation<X> getSideOutputTransform(OutputTag<X> outputTag) {
        TypeInformation<?> type = requestedSideOutputs.get(outputTag);
        if (type != null && !type.equals(outputTag.getTypeInfo())) {
            throw new UnsupportedOperationException(
                    "A side output with a matching id was "
                            + "already requested with a different type. This is not allowed, side output "
                            + "ids need to be unique.");
        }
        requestedSideOutputs.put(outputTag, outputTag.getTypeInfo());

        return new SideOutputTransformation<>(getTransformation(), outputTag);
    }
}
