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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

/**
 * Class representing temporal table function over some history table. It takes one single argument,
 * the {@code timeAttribute}, for which it returns matching version of the {@code
 * underlyingHistoryTable}, from which this {@link TemporalTableFunction} was created.
 *
 * <p>This function shouldn't be evaluated. Instead calls to it should be rewritten by the optimiser
 * into other operators (like Temporal Table Join).
 */
@Internal
public final class TemporalTableFunctionImpl extends TemporalTableFunction {

    private final transient QueryOperation underlyingHistoryTable;
    private final transient Expression timeAttribute;
    private final transient Expression primaryKey;
    private final TypeInformation<Row> resultType;

    private TemporalTableFunctionImpl(
            QueryOperation underlyingHistoryTable,
            Expression timeAttribute,
            Expression primaryKey,
            TypeInformation<Row> resultType) {
        this.underlyingHistoryTable = underlyingHistoryTable;
        this.timeAttribute = timeAttribute;
        this.primaryKey = primaryKey;
        this.resultType = resultType;
    }

    public void eval(Timestamp t) {
        throw new IllegalStateException("This should never be called");
    }

    public Expression getTimeAttribute() {
        return timeAttribute;
    }

    public Expression getPrimaryKey() {
        return primaryKey;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        final ResolvedSchema resolvedSchema = underlyingHistoryTable.getResolvedSchema();
        return TypeInference.newBuilder()
                .inputTypeStrategy(
                        InputTypeStrategies.or(
                                InputTypeStrategies.sequence(
                                        InputTypeStrategies.explicit(DataTypes.TIMESTAMP(3))),
                                InputTypeStrategies.sequence(
                                        InputTypeStrategies.explicit(DataTypes.TIMESTAMP_LTZ(3)))))
                .outputTypeStrategy(
                        TypeStrategies.explicit(
                                DataTypeUtils.fromResolvedSchemaPreservingTimeAttributes(
                                        resolvedSchema)))
                .build();
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return resultType;
    }

    public QueryOperation getUnderlyingHistoryTable() {
        if (underlyingHistoryTable == null) {
            throw new IllegalStateException("Accessing table field after planing/serialization");
        }
        return underlyingHistoryTable;
    }

    @SuppressWarnings("unchecked")
    public static TemporalTableFunction create(
            QueryOperation operationTree, Expression timeAttribute, Expression primaryKey) {
        return new TemporalTableFunctionImpl(
                operationTree,
                timeAttribute,
                primaryKey,
                (TypeInformation<Row>)
                        TypeConversions.fromDataTypeToLegacyInfo(
                                DataTypeUtils.fromResolvedSchemaPreservingTimeAttributes(
                                        operationTree.getResolvedSchema())));
    }
}
