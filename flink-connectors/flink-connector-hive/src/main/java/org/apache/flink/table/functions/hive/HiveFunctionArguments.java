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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;

import java.io.Serializable;
import java.util.BitSet;

/** Stores arguments information for a Hive function . */
public class HiveFunctionArguments implements Serializable {

    private static final long serialVersionUID = 1L;

    // input arguments -- store the value if an argument is literal, null otherwise
    private final Object[] args;
    // date type of each argument
    private final DataType[] argTypes;
    // store the indices of literal arguments, so that we can support null literals
    private final BitSet literalIndices;

    private HiveFunctionArguments(Object[] args, DataType[] argTypes, BitSet literalIndices) {
        this.args = args;
        this.argTypes = argTypes;
        this.literalIndices = literalIndices;
    }

    public int size() {
        return args.length;
    }

    public boolean isLiteral(int pos) {
        return pos >= 0 && pos < args.length && literalIndices.get(pos);
    }

    public Object getArg(int pos) {
        return args[pos];
    }

    public DataType getDataType(int pos) {
        return argTypes[pos];
    }

    // create from a CallContext
    public static HiveFunctionArguments create(CallContext callContext) {
        DataType[] argTypes = callContext.getArgumentDataTypes().toArray(new DataType[0]);
        Object[] args = new Object[argTypes.length];
        BitSet literalIndices = new BitSet(args.length);
        for (int i = 0; i < args.length; i++) {
            if (callContext.isArgumentLiteral(i)) {
                literalIndices.set(i);
                args[i] =
                        callContext
                                .getArgumentValue(
                                        i, argTypes[i].getLogicalType().getDefaultConversion())
                                .orElse(null);
                // we always use string type for string constant arg because that's what hive UDFs
                // expect.
                // it may happen that the type is char when call the function
                // in Flink SQL for calcite treat string literal as char type.
                if (args[i] instanceof String) {
                    argTypes[i] = DataTypes.STRING();
                }
            }
        }
        return new HiveFunctionArguments(args, argTypes, literalIndices);
    }
}
