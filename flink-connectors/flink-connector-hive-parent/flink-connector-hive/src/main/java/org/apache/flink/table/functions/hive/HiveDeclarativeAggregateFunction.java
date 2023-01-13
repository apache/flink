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

package org.apache.flink.table.functions.hive;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.DeclarativeAggregateFunction;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategy;

import java.util.Optional;

/**
 * API for hive aggregation functions that are expressed in terms of expressions.
 *
 * <p>Compared to {@link DeclarativeAggregateFunction}, this API provides extra {@link
 * #setArguments} method that is used to set input arguments information before call other methods.
 * We need this API because the aggBuffer and result type of some hive aggregate functions such as
 * sum is decided by the input type, this can be achieved by implements {@link #setArguments} method
 * correctly.
 *
 * <p>Moreover, if the hive aggregate function implements this API, planner will use the hash
 * aggregate strategy instead of sort aggregation that performance will be better.
 */
@Internal
public abstract class HiveDeclarativeAggregateFunction extends DeclarativeAggregateFunction {

    /**
     * Set input arguments for the function if need some inputs to infer the aggBuffer type and
     * result type. Otherwise, just implements it do nothing.
     */
    public abstract void setArguments(CallContext callContext);

    /** This method is used to infer result type when generate {@code AggregateCall} of calcite. */
    public TypeInference getTypeInference(DataTypeFactory factory) {
        return TypeInference.newBuilder()
                .outputTypeStrategy(new HiveAggregateFunctionOutputStrategy(this))
                .build();
    }

    @Override
    public FunctionKind getKind() {
        return FunctionKind.AGGREGATE;
    }

    /** OutputTypeStrategy for native hive aggregate function. */
    class HiveAggregateFunctionOutputStrategy implements TypeStrategy {

        private final HiveDeclarativeAggregateFunction declarativeAggregateFunction;

        public HiveAggregateFunctionOutputStrategy(
                HiveDeclarativeAggregateFunction declarativeAggregateFunction) {
            this.declarativeAggregateFunction = declarativeAggregateFunction;
        }

        @Override
        public Optional<DataType> inferType(CallContext callContext) {
            // set the input arguments first if you need some info to infer the aggBuffer/return
            // type
            declarativeAggregateFunction.setArguments(callContext);
            return Optional.of(declarativeAggregateFunction.getResultType());
        }
    }
}
