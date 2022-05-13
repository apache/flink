/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.legacyutils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedFieldReference;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.sources.tsextractors.TimestampExtractor;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.Preconditions;

/** A timestamp extractor that looks for the SQL_TIMESTAMP "ts" field. */
public class CustomExtractor extends TimestampExtractor {
    private static final long serialVersionUID = 6784900460276023738L;

    private final String field = "ts";

    @Override
    public String[] getArgumentFields() {
        return new String[] {field};
    }

    @Override
    public void validateArgumentFields(TypeInformation<?>[] argumentFieldTypes) {
        if (argumentFieldTypes[0] != Types.SQL_TIMESTAMP) {
            throw new ValidationException(
                    String.format(
                            "Field 'ts' must be of type Timestamp but is of type %s.",
                            argumentFieldTypes[0]));
        }
    }

    @Override
    public Expression getExpression(ResolvedFieldReference[] fieldAccesses) {
        ResolvedFieldReference fieldAccess = fieldAccesses[0];
        Preconditions.checkArgument(fieldAccess.resultType() == Types.SQL_TIMESTAMP);
        FieldReferenceExpression fieldReferenceExpr =
                new FieldReferenceExpression(
                        fieldAccess.name(),
                        TypeConversions.fromLegacyInfoToDataType(fieldAccess.resultType()),
                        0,
                        fieldAccess.fieldIndex());
        return ApiExpressionUtils.unresolvedCall(
                BuiltInFunctionDefinitions.CAST,
                fieldReferenceExpr,
                ApiExpressionUtils.typeLiteral(DataTypes.BIGINT()));
    }
}
