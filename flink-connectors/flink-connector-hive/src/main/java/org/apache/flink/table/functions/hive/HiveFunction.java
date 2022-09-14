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
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategy;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Interface for Hive UDF, UDTF, UDAF. */
@Internal
public interface HiveFunction<UDFType> {

    /** Sets input arguments for the function. */
    void setArguments(CallContext callContext);

    /**
     * Infers the return type of the function. This method should be called after {@link
     * #setArguments(CallContext)} is called.
     *
     * @return The inferred return type.
     * @throws UDFArgumentException can be thrown if the input arguments are invalid.
     */
    DataType inferReturnType() throws UDFArgumentException;

    /** Gets the wrapper for the Hive function. */
    HiveFunctionWrapper<UDFType> getFunctionWrapper();

    /** Creates {@link TypeInference} for the function. */
    default TypeInference createTypeInference() {
        TypeInference.Builder builder = TypeInference.newBuilder();
        builder.inputTypeStrategy(new HiveFunctionInputStrategy(this));
        builder.outputTypeStrategy(new HiveFunctionOutputStrategy(this));
        return builder.build();
    }

    /** InputTypeStrategy for Hive UDF, UDTF, UDAF. */
    class HiveFunctionInputStrategy implements InputTypeStrategy {

        private final HiveFunction<?> hiveFunction;

        public HiveFunctionInputStrategy(HiveFunction<?> hiveFunction) {
            this.hiveFunction = hiveFunction;
        }

        @Override
        public ArgumentCount getArgumentCount() {
            return ConstantArgumentCount.any();
        }

        @Override
        public Optional<List<DataType>> inferInputTypes(
                CallContext callContext, boolean throwOnFailure) {
            hiveFunction.setArguments(callContext);
            try {
                hiveFunction.inferReturnType();
            } catch (UDFArgumentException e) {
                if (throwOnFailure) {
                    throw callContext.newValidationError(
                            "Cannot find a suitable Hive function from %s for the input arguments",
                            hiveFunction.getFunctionWrapper().getUDFClassName());
                } else {
                    return Optional.empty();
                }
            }
            return Optional.of(callContext.getArgumentDataTypes());
        }

        @Override
        public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
            return Collections.singletonList(Signature.of(Signature.Argument.of("*")));
        }
    }

    /** OutputTypeStrategy for Hive UDF, UDTF, UDAF. */
    class HiveFunctionOutputStrategy implements TypeStrategy {

        private final HiveFunction<?> hiveFunction;

        public HiveFunctionOutputStrategy(HiveFunction<?> hiveFunction) {
            this.hiveFunction = hiveFunction;
        }

        @Override
        public Optional<DataType> inferType(CallContext callContext) {
            hiveFunction.setArguments(callContext);
            try {
                return Optional.of(hiveFunction.inferReturnType());
            } catch (UDFArgumentException e) {
                throw new FlinkHiveUDFException(e);
            }
        }
    }
}
