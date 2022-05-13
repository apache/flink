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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.operators.BinaryOperatorInformation;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.operators.Keys.IncompatibleKeysException;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.base.CoGroupRawOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;

/**
 * A {@link DataSet} that is the result of a CoGroup transformation.
 *
 * @param <I1> The type of the first input DataSet of the CoGroup transformation.
 * @param <I2> The type of the second input DataSet of the CoGroup transformation.
 * @param <OUT> The type of the result of the CoGroup transformation.
 * @see DataSet
 */
@Internal
public class CoGroupRawOperator<I1, I2, OUT>
        extends TwoInputUdfOperator<I1, I2, OUT, CoGroupRawOperator<I1, I2, OUT>> {

    private final CoGroupFunction<I1, I2, OUT> function;

    private final Keys<I1> keys1;
    private final Keys<I2> keys2;

    private final String defaultName;

    public CoGroupRawOperator(
            DataSet<I1> input1,
            DataSet<I2> input2,
            Keys<I1> keys1,
            Keys<I2> keys2,
            CoGroupFunction<I1, I2, OUT> function,
            TypeInformation<OUT> returnType,
            String defaultName) {
        super(input1, input2, returnType);
        this.function = function;
        this.defaultName = defaultName;
        this.name = defaultName;

        if (keys1 == null || keys2 == null) {
            throw new NullPointerException();
        }

        this.keys1 = keys1;
        this.keys2 = keys2;

        extractSemanticAnnotationsFromUdf(function.getClass());
    }

    protected Keys<I1> getKeys1() {
        return this.keys1;
    }

    protected Keys<I2> getKeys2() {
        return this.keys2;
    }

    @Override
    protected org.apache.flink.api.common.operators.base.CoGroupRawOperatorBase<?, ?, OUT, ?>
            translateToDataFlow(Operator<I1> input1, Operator<I2> input2) {
        String name = getName() != null ? getName() : "CoGroup at " + defaultName;
        try {
            keys1.areCompatible(keys2);
        } catch (IncompatibleKeysException e) {
            throw new InvalidProgramException("The types of the key fields do not match.", e);
        }

        if (keys1 instanceof Keys.ExpressionKeys && keys2 instanceof Keys.ExpressionKeys) {
            try {
                keys1.areCompatible(keys2);
            } catch (IncompatibleKeysException e) {
                throw new InvalidProgramException("The types of the key fields do not match.", e);
            }

            int[] logicalKeyPositions1 = keys1.computeLogicalKeyPositions();
            int[] logicalKeyPositions2 = keys2.computeLogicalKeyPositions();

            CoGroupRawOperatorBase<I1, I2, OUT, CoGroupFunction<I1, I2, OUT>> po =
                    new CoGroupRawOperatorBase<>(
                            function,
                            new BinaryOperatorInformation<>(
                                    getInput1Type(), getInput2Type(), getResultType()),
                            logicalKeyPositions1,
                            logicalKeyPositions2,
                            name);

            // set inputs
            po.setFirstInput(input1);
            po.setSecondInput(input2);

            // set dop
            po.setParallelism(this.getParallelism());

            return po;

        } else {
            throw new UnsupportedOperationException("Unrecognized or incompatible key types.");
        }
    }

    @Override
    protected Function getFunction() {
        return function;
    }
}
