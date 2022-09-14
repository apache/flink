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

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.api.common.typeutils.base.LocalDateTimeSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.planner.plan.nodes.exec.serde.DataTypeJsonSerdeTest.PojoClass;
import org.apache.flink.table.planner.typeutils.DataViewUtils;
import org.apache.flink.table.runtime.typeutils.ExternalSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.SymbolType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.table.types.utils.DataTypeFactoryMock;
import org.apache.flink.table.utils.CatalogManagerMocks;
import org.apache.flink.types.Row;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.api.config.TableConfigOptions.CatalogPlanCompilation.ALL;
import static org.apache.flink.table.api.config.TableConfigOptions.CatalogPlanCompilation.IDENTIFIER;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.configuredSerdeContext;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.toJson;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.toObject;
import static org.apache.flink.table.utils.CatalogManagerMocks.preparedCatalogManager;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/** Tests for {@link LogicalType} serialization and deserialization. */
@Execution(CONCURRENT)
public class LogicalTypeJsonSerdeTest {

    @ParameterizedTest
    @MethodSource("testLogicalTypeSerde")
    public void testLogicalTypeSerde(LogicalType logicalType) throws IOException {
        final SerdeContext serdeContext = configuredSerdeContext();

        final String json = toJson(serdeContext, logicalType);
        final LogicalType actual = toObject(serdeContext, json, LogicalType.class);

        assertThat(actual).isEqualTo(logicalType);
    }

    @Test
    public void testIdentifierSerde() throws IOException {
        final DataTypeFactoryMock dataTypeFactoryMock = new DataTypeFactoryMock();
        final TableConfig tableConfig = TableConfig.getDefault();
        final CatalogManager catalogManager =
                preparedCatalogManager().dataTypeFactory(dataTypeFactoryMock).build();
        final SerdeContext serdeContext = configuredSerdeContext(catalogManager, tableConfig);

        // minimal plan content
        tableConfig.set(TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS, IDENTIFIER);
        final String minimalJson = toJson(serdeContext, STRUCTURED_TYPE);
        assertThat(minimalJson).isEqualTo("\"`default_catalog`.`default_database`.`MyType`\"");

        // catalog lookup with miss
        tableConfig.set(
                TableConfigOptions.PLAN_RESTORE_CATALOG_OBJECTS,
                TableConfigOptions.CatalogPlanRestore.IDENTIFIER);
        dataTypeFactoryMock.logicalType = Optional.empty();
        assertThatThrownBy(() -> toObject(serdeContext, minimalJson, LogicalType.class))
                .satisfies(anyCauseMatches(ValidationException.class, "No type found."));

        // catalog lookup
        tableConfig.set(
                TableConfigOptions.PLAN_RESTORE_CATALOG_OBJECTS,
                TableConfigOptions.CatalogPlanRestore.IDENTIFIER);
        dataTypeFactoryMock.logicalType = Optional.of(STRUCTURED_TYPE);
        assertThat(toObject(serdeContext, minimalJson, LogicalType.class))
                .isEqualTo(STRUCTURED_TYPE);

        // maximum plan content
        tableConfig.set(TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS, ALL);
        final String maximumJson = toJson(serdeContext, STRUCTURED_TYPE);
        final JsonNode maximumJsonNode =
                JacksonMapperFactory.createObjectMapper().readTree(maximumJson);
        assertThat(maximumJsonNode.get(LogicalTypeJsonSerializer.FIELD_NAME_ATTRIBUTES))
                .isNotNull();
        assertThat(maximumJsonNode.get(LogicalTypeJsonSerializer.FIELD_NAME_DESCRIPTION).asText())
                .isEqualTo("My original type.");

        // catalog lookup with miss
        tableConfig.set(
                TableConfigOptions.PLAN_RESTORE_CATALOG_OBJECTS,
                TableConfigOptions.CatalogPlanRestore.IDENTIFIER);
        dataTypeFactoryMock.logicalType = Optional.empty();
        assertThatThrownBy(() -> toObject(serdeContext, maximumJson, LogicalType.class))
                .satisfies(anyCauseMatches(ValidationException.class, "No type found."));

        // catalog lookup
        tableConfig.set(
                TableConfigOptions.PLAN_RESTORE_CATALOG_OBJECTS,
                TableConfigOptions.CatalogPlanRestore.IDENTIFIER);
        dataTypeFactoryMock.logicalType = Optional.of(UPDATED_STRUCTURED_TYPE);
        assertThat(toObject(serdeContext, maximumJson, LogicalType.class))
                .isEqualTo(UPDATED_STRUCTURED_TYPE);

        // no lookup
        tableConfig.set(
                TableConfigOptions.PLAN_RESTORE_CATALOG_OBJECTS,
                TableConfigOptions.CatalogPlanRestore.ALL);
        dataTypeFactoryMock.logicalType = Optional.of(UPDATED_STRUCTURED_TYPE);
        assertThat(toObject(serdeContext, maximumJson, LogicalType.class))
                .isEqualTo(STRUCTURED_TYPE);
    }

    // --------------------------------------------------------------------------------------------
    // Test data
    // --------------------------------------------------------------------------------------------

    private static final StructuredType STRUCTURED_TYPE =
            StructuredType.newBuilder(
                            ObjectIdentifier.of(
                                    CatalogManagerMocks.DEFAULT_CATALOG,
                                    CatalogManagerMocks.DEFAULT_DATABASE,
                                    "MyType"))
                    .description("My original type.")
                    .build();

    private static final StructuredType UPDATED_STRUCTURED_TYPE =
            StructuredType.newBuilder(
                            ObjectIdentifier.of(
                                    CatalogManagerMocks.DEFAULT_CATALOG,
                                    CatalogManagerMocks.DEFAULT_DATABASE,
                                    "MyType"))
                    .description("My original type with update description.")
                    .build();

    private static List<LogicalType> testLogicalTypeSerde() {
        final List<LogicalType> types =
                Arrays.asList(
                        new BooleanType(),
                        new TinyIntType(),
                        new SmallIntType(),
                        new IntType(),
                        new BigIntType(),
                        new FloatType(),
                        new DoubleType(),
                        new DecimalType(10),
                        new DecimalType(15, 5),
                        CharType.ofEmptyLiteral(),
                        new CharType(),
                        new CharType(5),
                        VarCharType.ofEmptyLiteral(),
                        new VarCharType(),
                        new VarCharType(5),
                        BinaryType.ofEmptyLiteral(),
                        new BinaryType(),
                        new BinaryType(100),
                        VarBinaryType.ofEmptyLiteral(),
                        new VarBinaryType(),
                        new VarBinaryType(100),
                        new DateType(),
                        new TimeType(),
                        new TimeType(3),
                        new TimestampType(),
                        new TimestampType(3),
                        new LocalZonedTimestampType(false, TimestampKind.PROCTIME, 3),
                        new TimestampType(false, TimestampKind.ROWTIME, 3),
                        new ZonedTimestampType(),
                        new ZonedTimestampType(3),
                        new ZonedTimestampType(false, TimestampKind.ROWTIME, 3),
                        new LocalZonedTimestampType(),
                        new LocalZonedTimestampType(3),
                        new LocalZonedTimestampType(false, TimestampKind.PROCTIME, 3),
                        new LocalZonedTimestampType(false, TimestampKind.ROWTIME, 3),
                        new DayTimeIntervalType(DayTimeIntervalType.DayTimeResolution.DAY_TO_HOUR),
                        new DayTimeIntervalType(
                                false, DayTimeIntervalType.DayTimeResolution.DAY_TO_HOUR, 3, 6),
                        new YearMonthIntervalType(
                                YearMonthIntervalType.YearMonthResolution.YEAR_TO_MONTH),
                        new YearMonthIntervalType(
                                false, YearMonthIntervalType.YearMonthResolution.MONTH, 2),
                        new ZonedTimestampType(),
                        new LocalZonedTimestampType(),
                        new LocalZonedTimestampType(false, TimestampKind.PROCTIME, 3),
                        new SymbolType<>(),
                        new ArrayType(new IntType(false)),
                        new ArrayType(new LocalZonedTimestampType(false, TimestampKind.ROWTIME, 3)),
                        new ArrayType(new ZonedTimestampType(false, TimestampKind.ROWTIME, 3)),
                        new ArrayType(new TimestampType()),
                        new ArrayType(CharType.ofEmptyLiteral()),
                        new ArrayType(VarCharType.ofEmptyLiteral()),
                        new ArrayType(BinaryType.ofEmptyLiteral()),
                        new ArrayType(VarBinaryType.ofEmptyLiteral()),
                        new MapType(new BigIntType(), new IntType(false)),
                        new MapType(
                                new TimestampType(false, TimestampKind.ROWTIME, 3),
                                new ZonedTimestampType()),
                        new MapType(CharType.ofEmptyLiteral(), CharType.ofEmptyLiteral()),
                        new MapType(VarCharType.ofEmptyLiteral(), VarCharType.ofEmptyLiteral()),
                        new MapType(BinaryType.ofEmptyLiteral(), BinaryType.ofEmptyLiteral()),
                        new MapType(VarBinaryType.ofEmptyLiteral(), VarBinaryType.ofEmptyLiteral()),
                        new MultisetType(new IntType(false)),
                        new MultisetType(new TimestampType()),
                        new MultisetType(new TimestampType(true, TimestampKind.ROWTIME, 3)),
                        new MultisetType(CharType.ofEmptyLiteral()),
                        new MultisetType(VarCharType.ofEmptyLiteral()),
                        new MultisetType(BinaryType.ofEmptyLiteral()),
                        new MultisetType(VarBinaryType.ofEmptyLiteral()),
                        RowType.of(new BigIntType(), new IntType(false), new VarCharType(200)),
                        RowType.of(
                                new LogicalType[] {
                                    new BigIntType(), new IntType(false), new VarCharType(200)
                                },
                                new String[] {"f1", "f2", "f3"}),
                        RowType.of(
                                new TimestampType(false, TimestampKind.ROWTIME, 3),
                                new TimestampType(false, TimestampKind.REGULAR, 3),
                                new ZonedTimestampType(false, TimestampKind.ROWTIME, 3),
                                new ZonedTimestampType(false, TimestampKind.REGULAR, 3),
                                new LocalZonedTimestampType(false, TimestampKind.ROWTIME, 3),
                                new LocalZonedTimestampType(false, TimestampKind.PROCTIME, 3),
                                new LocalZonedTimestampType(false, TimestampKind.REGULAR, 3)),
                        RowType.of(
                                CharType.ofEmptyLiteral(),
                                VarCharType.ofEmptyLiteral(),
                                BinaryType.ofEmptyLiteral(),
                                VarBinaryType.ofEmptyLiteral()),
                        // registered structured type
                        StructuredType.newBuilder(
                                        ObjectIdentifier.of("cat", "db", "structuredType"),
                                        PojoClass.class)
                                .attributes(
                                        Arrays.asList(
                                                new StructuredType.StructuredAttribute(
                                                        "f0", new IntType(true)),
                                                new StructuredType.StructuredAttribute(
                                                        "f1", new BigIntType(true)),
                                                new StructuredType.StructuredAttribute(
                                                        "f2", new VarCharType(200), "desc")))
                                .comparison(StructuredType.StructuredComparison.FULL)
                                .setFinal(false)
                                .setInstantiable(false)
                                .superType(
                                        StructuredType.newBuilder(
                                                        ObjectIdentifier.of(
                                                                "cat", "db", "structuredType2"))
                                                .attributes(
                                                        Collections.singletonList(
                                                                new StructuredType
                                                                        .StructuredAttribute(
                                                                        "f0",
                                                                        new BigIntType(false))))
                                                .build())
                                .description("description for StructuredType")
                                .build(),
                        // unregistered structured type
                        StructuredType.newBuilder(PojoClass.class)
                                .attributes(
                                        Arrays.asList(
                                                new StructuredType.StructuredAttribute(
                                                        "f0", new IntType(true)),
                                                new StructuredType.StructuredAttribute(
                                                        "f1", new BigIntType(true)),
                                                new StructuredType.StructuredAttribute(
                                                        "f2", new VarCharType(200), "desc")))
                                .build(),
                        // registered distinct type
                        DistinctType.newBuilder(
                                        ObjectIdentifier.of("cat", "db", "distinctType"),
                                        new VarCharType(5))
                                .build(),
                        DistinctType.newBuilder(
                                        ObjectIdentifier.of("cat", "db", "distinctType"),
                                        new VarCharType(false, 5))
                                .build(),
                        // custom RawType
                        new RawType<>(LocalDateTime.class, LocalDateTimeSerializer.INSTANCE),
                        // external RawType
                        new RawType<>(
                                Row.class,
                                ExternalSerializer.of(
                                        DataTypes.ROW(DataTypes.INT(), DataTypes.STRING()))));

        final List<LogicalType> mutableTypes = new ArrayList<>(types);

        // RawType for MapView
        addRawTypesForMapView(mutableTypes, new VarCharType(100), new VarCharType(100));
        addRawTypesForMapView(mutableTypes, new VarCharType(100), new BigIntType());
        addRawTypesForMapView(mutableTypes, new BigIntType(), new VarCharType(100));
        addRawTypesForMapView(mutableTypes, new BigIntType(), new BigIntType());

        // RawType for ListView
        addRawTypesForListView(mutableTypes, new VarCharType(100));
        addRawTypesForListView(mutableTypes, new BigIntType());

        // RawType for custom MapView
        mutableTypes.add(
                DataViewUtils.adjustDataViews(
                                MapView.newMapViewDataType(
                                        DataTypes.STRING().toInternal(),
                                        DataTypes.STRING().bridgedTo(byte[].class)),
                                false)
                        .getLogicalType());

        final List<LogicalType> allTypes = new ArrayList<>();
        // consider nullable
        for (LogicalType type : mutableTypes) {
            allTypes.add(type.copy(true));
            allTypes.add(type.copy(false));
        }
        // ignore nullable for NullType
        allTypes.add(new NullType());
        return allTypes;
    }

    private static void addRawTypesForMapView(
            List<LogicalType> types, LogicalType keyType, LogicalType valueType) {
        for (boolean hasStateBackedDataViews : Arrays.asList(true, false)) {
            for (boolean keyNullable : Arrays.asList(true, false)) {
                for (boolean isInternalKeyType : Arrays.asList(true, false)) {
                    for (boolean valueNullable : Arrays.asList(true, false)) {
                        for (boolean isInternalValueType : Arrays.asList(true, false)) {
                            final DataType viewDataType =
                                    DataViewUtils.adjustDataViews(
                                            MapView.newMapViewDataType(
                                                    convertToInternalTypeIfNeeded(
                                                            DataTypes.of(keyType.copy(keyNullable)),
                                                            isInternalKeyType),
                                                    convertToInternalTypeIfNeeded(
                                                            DataTypes.of(
                                                                    valueType.copy(valueNullable)),
                                                            isInternalValueType)),
                                            hasStateBackedDataViews);
                            types.add(viewDataType.getLogicalType());
                        }
                    }
                }
            }
        }
    }

    private static void addRawTypesForListView(List<LogicalType> types, LogicalType elementType) {
        for (boolean hasStateBackedDataViews : Arrays.asList(true, false)) {
            for (boolean elementNullable : Arrays.asList(true, false)) {
                for (boolean isInternalType : Arrays.asList(true, false)) {
                    final DataType viewDataType =
                            DataViewUtils.adjustDataViews(
                                    ListView.newListViewDataType(
                                            convertToInternalTypeIfNeeded(
                                                    DataTypes.of(elementType.copy(elementNullable)),
                                                    isInternalType)),
                                    hasStateBackedDataViews);
                    types.add(viewDataType.getLogicalType());
                }
            }
        }
    }

    private static DataType convertToInternalTypeIfNeeded(
            DataType dataType, boolean isInternalType) {
        return isInternalType ? dataType.toInternal() : dataType;
    }
}
