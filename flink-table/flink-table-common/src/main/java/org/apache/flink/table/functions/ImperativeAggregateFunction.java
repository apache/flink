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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.catalog.DataTypeFactory;

/**
 * Base class for user-defined {@link AggregateFunction} and {@link TableAggregateFunction}.
 *
 * <p>This class is used for unified handling of imperative aggregating functions. Concrete
 * implementations should extend from {@link AggregateFunction} or {@link TableAggregateFunction}.
 *
 * @param <T> final result type of the aggregation
 * @param <ACC> intermediate result type during the aggregation
 */
@PublicEvolving
public abstract class ImperativeAggregateFunction<T, ACC> extends UserDefinedFunction {

    /**
     * Creates and initializes the accumulator for this {@link ImperativeAggregateFunction}.
     *
     * <p>The accumulator is an intermediate data structure that stores the aggregated values until
     * a final aggregation result is computed.
     *
     * @return the accumulator with the initial value
     */
    public abstract ACC createAccumulator();

    /**
     * Returns the {@link TypeInformation} of the {@link ImperativeAggregateFunction}'s result.
     *
     * @return The {@link TypeInformation} of the {@link ImperativeAggregateFunction}'s result or
     *     <code>null</code> if the result type should be automatically inferred.
     * @deprecated This method uses the old type system and is based on the old reflective
     *     extraction logic. The method will be removed in future versions and is only called when
     *     using the deprecated {@code TableEnvironment.registerFunction(...)} method. The new
     *     reflective extraction logic (possibly enriched with {@link DataTypeHint} and {@link
     *     FunctionHint}) should be powerful enough to cover most use cases. For advanced users, it
     *     is possible to override {@link UserDefinedFunction#getTypeInference(DataTypeFactory)}.
     */
    @Deprecated
    public TypeInformation<T> getResultType() {
        return null;
    }

    /**
     * Returns the {@link TypeInformation} of the {@link ImperativeAggregateFunction}'s accumulator.
     *
     * @return The {@link TypeInformation} of the {@link ImperativeAggregateFunction}'s accumulator
     *     or <code>null</code> if the accumulator type should be automatically inferred.
     * @deprecated This method uses the old type system and is based on the old reflective
     *     extraction logic. The method will be removed in future versions and is only called when
     *     using the deprecated {@code TableEnvironment.registerFunction(...)} method. The new
     *     reflective extraction logic (possibly enriched with {@link DataTypeHint} and {@link
     *     FunctionHint}) should be powerful enough to cover most use cases. For advanced users, it
     *     is possible to override {@link UserDefinedFunction#getTypeInference(DataTypeFactory)}.
     */
    @Deprecated
    public TypeInformation<ACC> getAccumulatorType() {
        return null;
    }
}
