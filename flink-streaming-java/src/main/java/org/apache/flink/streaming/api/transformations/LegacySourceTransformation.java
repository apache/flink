/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamSource;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This represents a Source. This does not actually transform anything since it has no inputs but it
 * is the root {@code Transformation} of any topology.
 *
 * @param <T> The type of the elements that this source produces
 */
@Internal
public class LegacySourceTransformation<T> extends PhysicalTransformation<T>
        implements WithBoundedness {

    private final StreamOperatorFactory<T> operatorFactory;

    private Boundedness boundedness;

    /**
     * Creates a new {@code LegacySourceTransformation} from the given operator.
     *
     * @param name The name of the {@code LegacySourceTransformation}, this will be shown in
     *     Visualizations and the Log
     * @param operator The {@code StreamSource} that is the operator of this Transformation
     * @param outputType The type of the elements produced by this {@code
     *     LegacySourceTransformation}
     * @param parallelism The parallelism of this {@code LegacySourceTransformation}
     */
    public LegacySourceTransformation(
            String name,
            StreamSource<T, ?> operator,
            TypeInformation<T> outputType,
            int parallelism,
            Boundedness boundedness) {
        this(name, SimpleOperatorFactory.of(operator), outputType, parallelism, boundedness);
    }

    public LegacySourceTransformation(
            String name,
            StreamOperatorFactory<T> operatorFactory,
            TypeInformation<T> outputType,
            int parallelism,
            Boundedness boundedness) {
        super(name, outputType, parallelism);
        this.operatorFactory = checkNotNull(operatorFactory);
        this.boundedness = checkNotNull(boundedness);
    }

    /** Mutable for legacy sources in the Table API. */
    public void setBoundedness(Boundedness boundedness) {
        this.boundedness = boundedness;
    }

    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    @VisibleForTesting
    public StreamSource<T, ?> getOperator() {
        return (StreamSource<T, ?>) ((SimpleOperatorFactory) operatorFactory).getOperator();
    }

    /** Returns the {@code StreamOperatorFactory} of this {@code LegacySourceTransformation}. */
    public StreamOperatorFactory<T> getOperatorFactory() {
        return operatorFactory;
    }

    @Override
    public List<Transformation<?>> getTransitivePredecessors() {
        return Collections.singletonList(this);
    }

    @Override
    public List<Transformation<?>> getInputs() {
        return Collections.emptyList();
    }

    @Override
    public final void setChainingStrategy(ChainingStrategy strategy) {
        operatorFactory.setChainingStrategy(strategy);
    }
}
