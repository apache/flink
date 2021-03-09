/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.functions.inference;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil.ConstantLookupKey;
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil.FieldRefLookupKey;
import org.apache.flink.table.planner.plan.utils.LookupJoinUtil.LookupKey;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.calcite.rex.RexLiteral;

import java.util.AbstractList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getFieldTypes;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/** The {@link CallContext} of a {@link LookupTableSource} runtime function. */
@Internal
public class LookupCallContext extends AbstractSqlCallContext {

    private final Map<Integer, LookupKey> lookupKeys;

    private final int[] lookupKeyOrder;

    private final List<DataType> argumentDataTypes;

    private final DataType outputDataType;

    public LookupCallContext(
            DataTypeFactory dataTypeFactory,
            UserDefinedFunction function,
            LogicalType inputType,
            Map<Integer, LookupKey> lookupKeys,
            int[] lookupKeyOrder,
            LogicalType lookupType) {
        super(dataTypeFactory, function, function.functionIdentifier());
        this.lookupKeys = lookupKeys;
        this.lookupKeyOrder = lookupKeyOrder;
        this.argumentDataTypes =
                new AbstractList<DataType>() {
                    @Override
                    public DataType get(int index) {
                        final LookupKey key = getKey(index);
                        final LogicalType keyType;
                        if (key instanceof ConstantLookupKey) {
                            keyType = ((ConstantLookupKey) key).sourceType;
                        } else if (key instanceof FieldRefLookupKey) {
                            keyType = getFieldTypes(inputType).get(((FieldRefLookupKey) key).index);
                        } else {
                            throw new IllegalArgumentException();
                        }
                        return fromLogicalToDataType(keyType);
                    }

                    @Override
                    public int size() {
                        return lookupKeyOrder.length;
                    }
                };
        this.outputDataType = fromLogicalToDataType(lookupType);
    }

    @Override
    public boolean isArgumentLiteral(int pos) {
        return getKey(pos) instanceof ConstantLookupKey;
    }

    @Override
    public boolean isArgumentNull(int pos) {
        final ConstantLookupKey key = (ConstantLookupKey) getKey(pos);
        return key.literal.isNull();
    }

    @Override
    public <T> Optional<T> getArgumentValue(int pos, Class<T> clazz) {
        if (isArgumentNull(pos)) {
            return Optional.empty();
        }
        try {
            final ConstantLookupKey key = (ConstantLookupKey) getKey(pos);
            final RexLiteral literal = key.literal;
            return Optional.ofNullable(getLiteralValueAs(literal::getValueAs, clazz));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    @Override
    public List<DataType> getArgumentDataTypes() {
        return argumentDataTypes;
    }

    @Override
    public Optional<DataType> getOutputDataType() {
        return Optional.of(outputDataType);
    }

    // --------------------------------------------------------------------------------------------

    private LookupKey getKey(int pos) {
        final int index = lookupKeyOrder[pos];
        return lookupKeys.get(index);
    }
}
