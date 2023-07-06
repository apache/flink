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

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.inference.TypeStrategy;

import java.util.Optional;

/**
 * Entry point for specific type strategies not covered in {@link TypeStrategies}.
 *
 * <p>This primarily serves the purpose of reducing visibility of individual type strategy
 * implementations to avoid polluting the API classpath.
 */
@Internal
public final class SpecificTypeStrategies {

    /** See {@link UnusedTypeStrategy}. */
    public static final TypeStrategy UNUSED = new UnusedTypeStrategy();

    /** See {@link RowTypeStrategy}. */
    public static final TypeStrategy ROW = new RowTypeStrategy();

    /** See {@link RoundTypeStrategy}. */
    public static final TypeStrategy ROUND = new RoundTypeStrategy();

    /** See {@link MapTypeStrategy}. */
    public static final TypeStrategy MAP = new MapTypeStrategy();

    /** See {@link IfNullTypeStrategy}. */
    public static final TypeStrategy IF_NULL = new IfNullTypeStrategy();

    /** See {@link StringConcatTypeStrategy}. */
    public static final TypeStrategy STRING_CONCAT = new StringConcatTypeStrategy();

    /** See {@link ArrayTypeStrategy}. */
    public static final TypeStrategy ARRAY = new ArrayTypeStrategy();

    /** Type strategy specific for array element. */
    public static final TypeStrategy ARRAY_ELEMENT =
            callContext ->
                    Optional.of(
                            ((CollectionDataType) callContext.getArgumentDataTypes().get(0))
                                    .getElementDataType());

    /** See {@link GetTypeStrategy}. */
    public static final TypeStrategy GET = new GetTypeStrategy();

    /** See {@link DecimalModTypeStrategy}. */
    public static final TypeStrategy DECIMAL_MOD = new DecimalModTypeStrategy();

    /** See {@link DecimalDivideTypeStrategy}. */
    public static final TypeStrategy DECIMAL_DIVIDE = new DecimalDivideTypeStrategy();

    /** See {@link DecimalPlusTypeStrategy}. */
    public static final TypeStrategy DECIMAL_PLUS = new DecimalPlusTypeStrategy();

    /** See {@link AggDecimalPlusTypeStrategy}. */
    public static final TypeStrategy AGG_DECIMAL_PLUS = new AggDecimalPlusTypeStrategy();

    /** See {@link HiveAggDecimalPlusTypeStrategy}. */
    public static final TypeStrategy HIVE_AGG_DECIMAL_PLUS = new HiveAggDecimalPlusTypeStrategy();

    /** See {@link DecimalScale0TypeStrategy}. */
    public static final TypeStrategy DECIMAL_SCALE_0 = new DecimalScale0TypeStrategy();

    /** See {@link DecimalTimesTypeStrategy}. */
    public static final TypeStrategy DECIMAL_TIMES = new DecimalTimesTypeStrategy();

    /** See {@link SourceWatermarkTypeStrategy}. */
    public static final TypeStrategy SOURCE_WATERMARK = new SourceWatermarkTypeStrategy();

    /** See {@link CurrentWatermarkTypeStrategy}. */
    public static final TypeStrategy CURRENT_WATERMARK = new CurrentWatermarkTypeStrategy();

    /** See {@link InternalReplicateRowsTypeStrategy}. */
    public static final TypeStrategy INTERNAL_REPLICATE_ROWS =
            new InternalReplicateRowsTypeStrategy();

    /** See {@link ToTimestampLtzTypeStrategy}. */
    public static final TypeStrategy TO_TIMESTAMP_LTZ = new ToTimestampLtzTypeStrategy();

    /** Type strategy specific for {@link BuiltInFunctionDefinitions#MAP_KEYS}. */
    public static final TypeStrategy MAP_KEYS =
            callContext ->
                    Optional.of(
                            DataTypes.ARRAY(
                                    ((KeyValueDataType) callContext.getArgumentDataTypes().get(0))
                                            .getKeyDataType()));

    /** Type strategy specific for {@link BuiltInFunctionDefinitions#MAP_VALUES}. */
    public static final TypeStrategy MAP_VALUES =
            callContext ->
                    Optional.of(
                            DataTypes.ARRAY(
                                    ((KeyValueDataType) callContext.getArgumentDataTypes().get(0))
                                            .getValueDataType()));

    /** Type strategy specific for {@link BuiltInFunctionDefinitions#MAP_ENTRIES}. */
    public static final TypeStrategy MAP_ENTRIES =
            callContext ->
                    Optional.of(
                            DataTypes.ARRAY(
                                    DataTypes.ROW(
                                            DataTypes.FIELD(
                                                    "key",
                                                    ((KeyValueDataType)
                                                                    callContext
                                                                            .getArgumentDataTypes()
                                                                            .get(0))
                                                            .getKeyDataType()),
                                            DataTypes.FIELD(
                                                    "value",
                                                    ((KeyValueDataType)
                                                                    callContext
                                                                            .getArgumentDataTypes()
                                                                            .get(0))
                                                            .getValueDataType()))));

    /** Type strategy specific for {@link BuiltInFunctionDefinitions#MAP_FROM_ARRAYS}. */
    public static final TypeStrategy MAP_FROM_ARRAYS =
            callContext ->
                    Optional.of(
                            DataTypes.MAP(
                                    ((CollectionDataType) callContext.getArgumentDataTypes().get(0))
                                            .getElementDataType(),
                                    ((CollectionDataType) callContext.getArgumentDataTypes().get(1))
                                            .getElementDataType()));

    private SpecificTypeStrategies() {
        // no instantiation
    }
}
