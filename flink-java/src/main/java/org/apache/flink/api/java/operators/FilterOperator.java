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

package org.apache.flink.api.java.operators;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.translation.PlanFilterOperator;

/**
 * This operator represents the application of a "filter" function on a data set, and the result
 * data set produced by the function.
 *
 * @param <T> The type of the data set filtered by the operator.
 */
@Public
public class FilterOperator<T> extends SingleInputUdfOperator<T, T, FilterOperator<T>> {

    protected final FilterFunction<T> function;

    protected final String defaultName;

    public FilterOperator(DataSet<T> input, FilterFunction<T> function, String defaultName) {
        super(input, input.getType());

        this.function = function;
        this.defaultName = defaultName;
    }

    @Override
    protected FilterFunction<T> getFunction() {
        return function;
    }

    @Override
    protected org.apache.flink.api.common.operators.base.FilterOperatorBase<
                    T, FlatMapFunction<T, T>>
            translateToDataFlow(Operator<T> input) {

        String name = getName() != null ? getName() : "Filter at " + defaultName;

        // create operator
        PlanFilterOperator<T> po = new PlanFilterOperator<T>(function, name, getInputType());
        po.setInput(input);

        // set parallelism
        if (getParallelism() > 0) {
            // use specified parallelism
            po.setParallelism(getParallelism());
        } else {
            // if no parallelism has been specified, use parallelism of input operator to enable
            // chaining
            po.setParallelism(input.getParallelism());
        }

        return po;
    }
}
