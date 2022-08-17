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

package org.apache.flink.connector.mongodb.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.mongodb.common.utils.MongoValidationUtils;
import org.apache.flink.connector.mongodb.table.converter.RowDataToBsonConverters;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.function.SerializableFunction;

import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.types.ObjectId;

import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** An extractor for a MongoDB key from a {@link RowData}. */
@Internal
public class MongoKeyExtractor implements SerializableFunction<RowData, BsonValue> {

    public static final String RESERVED_ID = "_id";

    private final LogicalType primaryKeyType;

    private final int[] primaryKeyIndexes;

    private final RowDataToBsonConverters.RowDataToBsonConverter primaryKeyConverter;

    private MongoKeyExtractor(LogicalType primaryKeyType, int[] primaryKeyIndexes) {
        this.primaryKeyType = primaryKeyType;
        this.primaryKeyIndexes = primaryKeyIndexes;
        this.primaryKeyConverter = RowDataToBsonConverters.createNullableConverter(primaryKeyType);
    }

    @Override
    public BsonValue apply(RowData rowData) {
        BsonValue keyValue;
        if (isCompoundPrimaryKey(primaryKeyIndexes)) {
            RowData keyRow = ProjectedRowData.from(primaryKeyIndexes).replaceRow(rowData);
            keyValue = primaryKeyConverter.convert(keyRow);
        } else {
            RowData.FieldGetter fieldGetter =
                    RowData.createFieldGetter(primaryKeyType, primaryKeyIndexes[0]);
            keyValue = primaryKeyConverter.convert(fieldGetter.getFieldOrNull(rowData));
            if (keyValue.isString()) {
                String keyString = keyValue.asString().getValue();
                // Try to restore MongoDB's ObjectId from string.
                if (ObjectId.isValid(keyString)) {
                    keyValue = new BsonObjectId(new ObjectId(keyString));
                }
            }
        }
        return checkNotNull(keyValue, "Primary key value is null of RowData: " + rowData);
    }

    public static SerializableFunction<RowData, BsonValue> createKeyExtractor(
            ResolvedSchema resolvedSchema) {

        Optional<UniqueConstraint> primaryKey = resolvedSchema.getPrimaryKey();
        int[] primaryKeyIndexes = resolvedSchema.getPrimaryKeyIndexes();
        Optional<Column> reversedId = resolvedSchema.getColumn(RESERVED_ID);

        // It behaves as append-only when no primary key is declared and no reversed _id is present.
        // We use anonymous classes instead of lambdas for a reason here. It is
        // necessary because the maven shade plugin cannot relocate classes in SerializedLambdas
        // (MSHADE-260).
        if (!primaryKey.isPresent() && !reversedId.isPresent()) {
            return new SerializableFunction<RowData, BsonValue>() {
                private static final long serialVersionUID = 1L;

                @Override
                public BsonValue apply(RowData rowData) {
                    return null;
                }
            };
        }

        if (reversedId.isPresent()) {
            // Primary key should be declared as (_id) when the mongo reversed _id is present.
            if (!primaryKey.isPresent()
                    || isCompoundPrimaryKey(primaryKeyIndexes)
                    || !primaryKeyContainsReversedId(primaryKey.get())) {
                throw new IllegalArgumentException(
                        "The primary key should be declared as (_id) when mongo reversed _id field is present");
            }
        }

        DataType primaryKeyType;
        if (isCompoundPrimaryKey(primaryKeyIndexes)) {
            DataType physicalRowDataType = resolvedSchema.toPhysicalRowDataType();
            primaryKeyType = Projection.of(primaryKeyIndexes).project(physicalRowDataType);
        } else {
            int primaryKeyIndex = primaryKeyIndexes[0];
            Optional<Column> column = resolvedSchema.getColumn(primaryKeyIndex);
            if (!column.isPresent()) {
                throw new IllegalStateException(
                        String.format(
                                "No primary key column found with index '%s'.", primaryKeyIndex));
            }
            primaryKeyType = column.get().getDataType();
        }

        MongoValidationUtils.validatePrimaryKey(primaryKeyType);

        return new MongoKeyExtractor(primaryKeyType.getLogicalType(), primaryKeyIndexes);
    }

    private static boolean isCompoundPrimaryKey(int[] primaryKeyIndexes) {
        return primaryKeyIndexes.length > 1;
    }

    private static boolean primaryKeyContainsReversedId(UniqueConstraint primaryKey) {
        return primaryKey.getColumns().contains(RESERVED_ID);
    }
}
