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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;

/**
 * Base class for operations that operates on a single input data set.
 *
 * @param <IN> The data type of the input data set.
 * @param <OUT> The data type of the returned data set.
 * @deprecated All Flink DataSet APIs are deprecated since Flink 1.18 and will be removed in a
 *     future Flink major version. You can still build your application in DataSet, but you should
 *     move to either the DataStream and/or Table API.
 * @see <a href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=158866741">
 *     FLIP-131: Consolidate the user-facing Dataflow SDKs/APIs (and deprecate the DataSet API</a>
 */
@Deprecated
@Public
public abstract class SingleInputOperator<IN, OUT, O extends SingleInputOperator<IN, OUT, O>>
        extends Operator<OUT, O> {

    private final DataSet<IN> input;

    protected SingleInputOperator(DataSet<IN> input, TypeInformation<OUT> resultType) {
        super(input.getExecutionEnvironment(), resultType);
        this.input = input;
    }

    /**
     * Gets the data set that this operation uses as its input.
     *
     * @return The data set that this operation uses as its input.
     */
    public DataSet<IN> getInput() {
        return this.input;
    }

    /**
     * Gets the type information of the data type of the input data set. This method returns
     * equivalent information as {@code getInput().getType()}.
     *
     * @return The input data type.
     */
    public TypeInformation<IN> getInputType() {
        return this.input.getType();
    }

    /**
     * Translates this operation to a data flow operator of the common data flow API.
     *
     * @param input The data flow operator that produces this operation's input data.
     * @return The translated data flow operator.
     */
    protected abstract org.apache.flink.api.common.operators.Operator<OUT> translateToDataFlow(
            org.apache.flink.api.common.operators.Operator<IN> input);
}
