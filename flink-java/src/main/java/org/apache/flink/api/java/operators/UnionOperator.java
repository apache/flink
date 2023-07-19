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
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.Union;
import org.apache.flink.api.java.DataSet;

/**
 * Java API operator for union of two data sets.
 *
 * @param <T> The type of the two input data sets and the result data set
 * @deprecated All Flink DataSet APIs are deprecated since Flink 1.18 and will be removed in a
 *     future Flink major version. You can still build your application in DataSet, but you should
 *     move to either the DataStream and/or Table API.
 * @see <a href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=158866741">
 *     FLIP-131: Consolidate the user-facing Dataflow SDKs/APIs (and deprecate the DataSet API</a>
 */
@Deprecated
@Public
public class UnionOperator<T> extends TwoInputOperator<T, T, T, UnionOperator<T>> {

    private final String unionLocationName;

    /**
     * Create an operator that produces the union of the two given data sets.
     *
     * @param input1 The first data set to be unioned.
     * @param input2 The second data set to be unioned.
     */
    public UnionOperator(DataSet<T> input1, DataSet<T> input2, String unionLocationName) {
        super(input1, input2, input1.getType());

        if (!input1.getType().equals(input2.getType())) {
            throw new InvalidProgramException(
                    "Cannot union inputs of different types. Input1="
                            + input1.getType()
                            + ", input2="
                            + input2.getType());
        }

        this.unionLocationName = unionLocationName;
    }

    /**
     * Returns the BinaryNodeTranslation of the Union.
     *
     * @param input1 The first input of the union, as a common API operator.
     * @param input2 The second input of the union, as a common API operator.
     * @return The common API union operator.
     */
    @Override
    protected Union<T> translateToDataFlow(Operator<T> input1, Operator<T> input2) {
        return new Union<T>(input1, input2, unionLocationName);
    }

    @Override
    public UnionOperator<T> setParallelism(int parallelism) {
        // Union is not translated to an independent operator but executed by multiplexing
        // its input on the following operator. Hence, the parallelism of a Union cannot be set.
        throw new UnsupportedOperationException("Cannot set the parallelism for Union.");
    }
}
