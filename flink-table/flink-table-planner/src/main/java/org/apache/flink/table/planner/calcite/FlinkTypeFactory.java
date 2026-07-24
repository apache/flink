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

package org.apache.flink.table.planner.calcite;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.NothingTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.calcite.ExtendedRelTypeFactory;
import org.apache.flink.table.legacy.api.TableSchema;
import org.apache.flink.table.legacy.types.logical.TypeInformationRawType;
import org.apache.flink.table.planner.plan.schema.BitmapRelDataType;
import org.apache.flink.table.planner.plan.schema.GenericRelDataType;
import org.apache.flink.table.planner.plan.schema.GeographyRelDataType;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;
import org.apache.flink.table.planner.plan.schema.StructuredRelDataType;
import org.apache.flink.table.planner.plan.schema.TimeIndicatorRelDataType;
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter;
import org.apache.flink.table.runtime.types.PlannerTypeUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BitmapType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DescriptorType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.GeographyType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
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
import org.apache.flink.table.types.logical.VariantType;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.MapSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ConversionUtil;

import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import scala.collection.JavaConverters;
import scala.collection.Seq;

import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP;
import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
import static org.apache.calcite.sql.type.SqlTypeName.VARBINARY;

/**
 * Flink specific type factory that represents the interface between Flink's {@link LogicalType} and
 * Calcite's {@link RelDataType}.
 */
public class FlinkTypeFactory extends JavaTypeFactoryImpl implements ExtendedRelTypeFactory {
    private final Map<LogicalType, RelDataType> seenTypes = new HashMap<>();
    private final ClassLoader classLoader;

    public FlinkTypeFactory(ClassLoader classLoader, RelDataTypeSystem typeSystem) {
        super(typeSystem);
        this.classLoader = classLoader;
    }

    public FlinkTypeFactory(ClassLoader classLoader) {
        this(classLoader, FlinkTypeSystem.INSTANCE);
    }

    @Override
    public RelDataType createRawType(String className, String serializerString) {
        final RawType rawType = RawType.restore(classLoader, className, serializerString);
        final RelDataType rawRelDataType = createFieldTypeFromLogicalType(rawType);
        return canonize(rawRelDataType);
    }

    @Override
    public RelDataType createStructuredType(
            String className, List<RelDataType> fieldTypes, List<String> fieldNames) {
        final Optional<Class<?>> resolvedClass =
                StructuredType.resolveClass(classLoader, className);
        final StructuredType.Builder builder =
                resolvedClass
                        .map(StructuredType::newBuilder)
                        .orElseGet(() -> StructuredType.newBuilder(className));

        final List<RelDataTypeField> relFields = new ArrayList<>();
        for (int i = 0; i < fieldTypes.size(); i++) {
            relFields.add(new RelDataTypeFieldImpl(fieldNames.get(i), i, fieldTypes.get(i)));
        }

        builder.attributes(
                relFields.stream()
                        .map(
                                f ->
                                        new StructuredType.StructuredAttribute(
                                                f.getName(), toLogicalType(f.getType())))
                        .collect(Collectors.toList()));

        final RelDataType relDataType = new StructuredRelDataType(builder.build(), relFields);
        return canonize(relDataType);
    }

    @Override
    public RelDataType createBitmapType() {
        return canonize(new BitmapRelDataType(new BitmapType()));
    }

    @Override
    public RelDataType createGeographyType() {
        return canonize(new GeographyRelDataType(new GeographyType()));
    }

    @Override
    public RelDataType createArrayType(RelDataType elementType, long maxCardinality) {
        // Just validate type, make sure there is a failure in validate phase.
        checkForNullType(elementType);
        toLogicalType(elementType);
        return super.createArrayType(elementType, maxCardinality);
    }

    @Override
    public RelDataType createMapType(RelDataType keyType, RelDataType valueType) {
        // Just validate type, make sure there is a failure in validate phase.
        checkForNullType(keyType, valueType);
        toLogicalType(keyType);
        toLogicalType(valueType);
        return super.createMapType(keyType, valueType);
    }

    @Override
    public RelDataType createMultisetType(RelDataType elementType, long maxCardinality) {
        // Just validate type, make sure there is a failure in validate phase.
        checkForNullType(elementType);
        toLogicalType(elementType);
        return super.createMultisetType(elementType, maxCardinality);
    }

    @Override
    public Type getJavaClass(RelDataType type) {
        if (type.getSqlTypeName() == SqlTypeName.FLOAT) {
            return type.isNullable() ? Float.class : Float.TYPE;
        }
        return super.getJavaClass(type);
    }

    @Override
    public RelDataType createSqlType(SqlTypeName typeName) {
        if (typeName == SqlTypeName.DECIMAL) {
            // if we got here, the precision and scale are not specified, here we
            // keep precision/scale in sync with our type system's default value,
            // see DecimalType.USER_DEFAULT.
            return createSqlType(
                    typeName, DecimalType.DEFAULT_PRECISION, DecimalType.DEFAULT_SCALE);
        }
        return super.createSqlType(typeName);
    }

    @Override
    public RelDataType createSqlType(SqlTypeName typeName, int precision) {
        // it might happen that inferred VARCHAR types overflow as we set them to Int.MaxValue
        // Calcite will limit the length of the VARCHAR type to 65536.
        if (typeName == SqlTypeName.VARCHAR && precision < 0) {
            return createSqlType(typeName, getTypeSystem().getDefaultPrecision(typeName));
        }
        return super.createSqlType(typeName, precision);
    }

    @Override
    public RelDataType createTypeWithNullability(RelDataType relDataType, boolean isNullable) {
        // nullability change not necessary
        if (relDataType.isNullable() == isNullable) {
            return canonize(relDataType);
        }

        // change nullability
        final RelDataType newType;
        if (relDataType instanceof RawRelDataType) {
            newType = ((RawRelDataType) relDataType).createWithNullability(isNullable);
        } else if (relDataType instanceof StructuredRelDataType) {
            newType = ((StructuredRelDataType) relDataType).createWithNullability(isNullable);
        } else if (relDataType instanceof BitmapRelDataType) {
            newType = ((BitmapRelDataType) relDataType).createWithNullability(isNullable);
        } else if (relDataType instanceof GeographyRelDataType) {
            newType = ((GeographyRelDataType) relDataType).createWithNullability(isNullable);
        } else if (relDataType instanceof GenericRelDataType) {
            final GenericRelDataType generic = (GenericRelDataType) relDataType;
            newType = new GenericRelDataType(generic.genericType(), isNullable, getTypeSystem());
        } else if (relDataType instanceof TimeIndicatorRelDataType) {
            final TimeIndicatorRelDataType it = (TimeIndicatorRelDataType) relDataType;
            newType =
                    new TimeIndicatorRelDataType(
                            it.getTypeSystem(), it.getOriginalType(), isNullable, it.isEventTime());
        } else if (relDataType instanceof RelRecordType
                && relDataType.getStructKind() == StructKind.PEEK_FIELDS_NO_EXPAND) {
            // for nested rows we keep the nullability property,
            // top-level rows fall back to Calcite's default handling
            final RelRecordType rt = (RelRecordType) relDataType;
            newType = new RelRecordType(rt.getStructKind(), rt.getFieldList(), isNullable);
        } else {
            newType = super.createTypeWithNullability(relDataType, isNullable);
        }

        return canonize(newType);
    }

    @Override
    public RelDataType leastRestrictive(List<RelDataType> types) {
        final Optional<RelDataType> resolved = resolveAllIdenticalTypes(types);
        if (resolved.isPresent()) {
            return normalizeLeastRestrictive(resolved.get());
        }

        if (containsFlinkExtensionType(types)) {
            return normalizeLeastRestrictive(
                    resolveCommonTypeForFlinkExtensions(types).orElse(null));
        }

        final RelDataType leastRestrictive = super.leastRestrictive(types);
        return normalizeLeastRestrictive(leastRestrictive);
    }

    private RelDataType normalizeLeastRestrictive(RelDataType leastRestrictive) {
        // NULL is reserved for untyped literals only
        if (leastRestrictive == null || leastRestrictive.getSqlTypeName() == SqlTypeName.NULL) {
            return null;
        }
        return leastRestrictive;
    }

    private Optional<RelDataType> resolveCommonTypeForFlinkExtensions(List<RelDataType> types) {
        return LogicalTypeMerging.findCommonType(
                        types.stream()
                                .map(FlinkTypeFactory::toLogicalType)
                                .collect(Collectors.toList()))
                .map(this::createFieldTypeFromLogicalType);
    }

    private boolean containsFlinkExtensionType(List<RelDataType> types) {
        return types.stream().anyMatch(FlinkTypeFactory::isFlinkExtensionType);
    }

    private static boolean isFlinkExtensionType(RelDataType type) {
        return type instanceof RawRelDataType
                || type instanceof BitmapRelDataType
                || type instanceof GeographyRelDataType;
    }

    private Optional<RelDataType> resolveAllIdenticalTypes(List<RelDataType> types) {
        final RelDataType head = types.get(0);
        // check if all types are the same
        if (types.stream().allMatch(t -> t.equals(head))) {
            // types are the same, check nullability
            final boolean nullable =
                    types.stream()
                            .anyMatch(
                                    sqlType ->
                                            sqlType.isNullable()
                                                    || sqlType.getSqlTypeName()
                                                            == SqlTypeName.NULL);
            // return type with nullability
            return Optional.of(createTypeWithNullability(head, nullable));
        } else {
            // types are not all the same
            if (types.stream().anyMatch(t -> t.getSqlTypeName() == SqlTypeName.ANY)) {
                // one of the type was RAW.
                // we cannot generate a common type if it differs from other types.
                throw new TableException("Generic RAW types must have a common type information.");
            } else {
                // cannot resolve a common type for different input types
                return Optional.empty();
            }
        }
    }

    @Override
    public Charset getDefaultCharset() {
        return Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME);
    }

    /**
     * Creates a struct type with the input fieldNames and input fieldTypes using FlinkTypeFactory.
     *
     * @param fieldNames field names
     * @param fieldTypes field types, every element is Flink's {@link LogicalType}.
     * @param structKind Name resolution policy. See more information in {@link StructKind}.
     * @return a struct type with the input fieldNames, input fieldTypes.
     */
    private RelDataType buildStructType(
            String[] fieldNames, LogicalType[] fieldTypes, StructKind structKind) {
        RelDataTypeFactory.Builder b = builder();
        b.kind(structKind);
        for (int i = 0; i < fieldTypes.length; i++) {
            final LogicalType fieldType = fieldTypes[i];
            final String fieldName = fieldNames[i];
            final RelDataType fieldRelDataType = createFieldTypeFromLogicalType(fieldType);
            checkForNullType(fieldRelDataType);
            b.add(fieldName, fieldRelDataType);
        }
        return b.build();
    }

    /**
     * Create a calcite field type in table schema from {@link LogicalType}. It uses
     * PEEK_FIELDS_NO_EXPAND when type is a nested struct type (Flink {@link RowType}).
     *
     * @param type Flink logical type.
     * @return calcite {@link RelDataType}.
     */
    public RelDataType createFieldTypeFromLogicalType(LogicalType type) {

        // Kind in TimestampType do not affect the hashcode and equals, So we can't put it to
        // seenTypes
        final RelDataType relType;
        switch (type.getTypeRoot()) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final TimestampType timestampType = (TimestampType) type;
                switch (timestampType.getKind()) {
                    case ROWTIME:
                        relType = createRowtimeIndicatorType(type.isNullable(), false);
                        break;
                    case REGULAR:
                        relType = createSqlType(TIMESTAMP, timestampType.getPrecision());
                        break;
                    case PROCTIME:
                        throw new TableException(
                                "Processing time indicator only supports"
                                        + " LocalZonedTimestampType, but actual is TimestampType."
                                        + " This is a bug in planner, please file an issue.");
                    default:
                        throw new TableException(
                                "Unsupported TimestampKind: " + timestampType.getKind());
                }
                break;

            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final LocalZonedTimestampType lzTs = (LocalZonedTimestampType) type;
                switch (lzTs.getKind()) {
                    case PROCTIME:
                        relType = createProctimeIndicatorType(type.isNullable());
                        break;
                    case ROWTIME:
                        relType = createRowtimeIndicatorType(type.isNullable(), true);
                        break;
                    case REGULAR:
                        relType =
                                createSqlType(TIMESTAMP_WITH_LOCAL_TIME_ZONE, lzTs.getPrecision());
                        break;
                    default:
                        throw new TableException("Unsupported TimestampKind: " + lzTs.getKind());
                }
                break;
            default:
                final RelDataType seenType = seenTypes.get(type);
                if (seenType != null) {
                    relType = seenType;
                } else {
                    final RelDataType refType = newRelDataType(type);
                    seenTypes.put(type, refType);
                    relType = refType;
                }
        }

        return createTypeWithNullability(relType, type.isNullable());
    }

    private RelDataType newRelDataType(LogicalType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case NULL:
                return createSqlType(SqlTypeName.NULL);
            case BOOLEAN:
                return createSqlType(SqlTypeName.BOOLEAN);
            case TINYINT:
                return createSqlType(SqlTypeName.TINYINT);
            case SMALLINT:
                return createSqlType(SqlTypeName.SMALLINT);
            case INTEGER:
                return createSqlType(SqlTypeName.INTEGER);
            case BIGINT:
                return createSqlType(SqlTypeName.BIGINT);
            case FLOAT:
                return createSqlType(SqlTypeName.FLOAT);
            case DOUBLE:
                return createSqlType(SqlTypeName.DOUBLE);
            case VARCHAR:
                return createSqlType(SqlTypeName.VARCHAR, ((VarCharType) logicalType).getLength());
            case CHAR:
                return createSqlType(SqlTypeName.CHAR, ((CharType) logicalType).getLength());

            // temporal types
            case DATE:
                return createSqlType(SqlTypeName.DATE);
            case TIME_WITHOUT_TIME_ZONE:
                return createSqlType(SqlTypeName.TIME, ((TimeType) logicalType).getPrecision());

            // interval types
            case INTERVAL_YEAR_MONTH:
                return createSqlIntervalType(
                        new SqlIntervalQualifier(TimeUnit.YEAR, TimeUnit.MONTH, SqlParserPos.ZERO));
            case INTERVAL_DAY_TIME:
                return createSqlIntervalType(
                        new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.SECOND, SqlParserPos.ZERO));

            case BINARY:
                return createSqlType(SqlTypeName.BINARY, ((BinaryType) logicalType).getLength());
            case VARBINARY:
                return createSqlType(VARBINARY, ((VarBinaryType) logicalType).getLength());

            case DECIMAL:
                if (logicalType instanceof DecimalType) {
                    DecimalType decimalType = (DecimalType) logicalType;
                    return createSqlType(
                            DECIMAL, decimalType.getPrecision(), decimalType.getScale());
                } else if (logicalType instanceof LegacyTypeInformationType) {
                    LegacyTypeInformationType<?> legacyType =
                            (LegacyTypeInformationType<?>) logicalType;
                    if (legacyType.getTypeInformation() == BasicTypeInfo.BIG_DEC_TYPE_INFO) {
                        return createSqlType(DECIMAL, 38, 18);
                    }
                }
                throw new TableException("Type is not supported: " + logicalType);
            case ROW:
                final RowType rowType = (RowType) logicalType;
                return buildStructType(
                        rowType.getFieldNames().toArray(new String[0]),
                        rowType.getChildren().toArray(new LogicalType[0]),
                        // fields are not expanded in "SELECT *"
                        StructKind.PEEK_FIELDS_NO_EXPAND);

            case STRUCTURED_TYPE:
                if (logicalType instanceof StructuredType) {
                    return StructuredRelDataType.create(this, (StructuredType) logicalType);
                } else if (logicalType instanceof LegacyTypeInformationType) {
                    return createFieldTypeFromLogicalType(
                            PlannerTypeUtils.removeLegacyTypes(logicalType));
                }
                throw new TableException("Type is not supported: " + logicalType);
            case ARRAY:
                final ArrayType arrayType = (ArrayType) logicalType;
                return createArrayType(
                        createFieldTypeFromLogicalType(arrayType.getElementType()), -1);

            case MAP:
                final MapType mapType = (MapType) logicalType;
                return createMapType(
                        createFieldTypeFromLogicalType(mapType.getKeyType()),
                        createFieldTypeFromLogicalType(mapType.getValueType()));

            case MULTISET:
                final MultisetType multisetType = (MultisetType) logicalType;
                return createMultisetType(
                        createFieldTypeFromLogicalType(multisetType.getElementType()), -1);

            case RAW:
                if (logicalType instanceof RawType) {
                    return new RawRelDataType((RawType<?>) logicalType);
                } else if (logicalType instanceof TypeInformationRawType) {
                    return new GenericRelDataType(
                            (TypeInformationRawType<?>) logicalType, true, getTypeSystem());
                } else if (logicalType instanceof LegacyTypeInformationType) {
                    return createFieldTypeFromLogicalType(
                            PlannerTypeUtils.removeLegacyTypes(logicalType));
                }
                throw new TableException("Type is not supported: " + logicalType);

            case SYMBOL:
                return createSqlType(SqlTypeName.SYMBOL);

            case DESCRIPTOR:
                return createSqlType(SqlTypeName.COLUMN_LIST);

            case VARIANT:
                return createSqlType(SqlTypeName.VARIANT);

            case BITMAP:
                return new BitmapRelDataType((BitmapType) logicalType);

            case GEOGRAPHY:
                return new GeographyRelDataType((GeographyType) logicalType);

            default:
                throw new TableException("Type is not supported: " + logicalType);
        }
    }

    /**
     * This is a safety check in case the null type ends up in the type factory for other use cases
     * than untyped NULL literals.
     */
    private void checkForNullType(RelDataType... childTypes) {
        for (RelDataType childType : childTypes) {

            if (childType.getSqlTypeName() == SqlTypeName.NULL) {
                throw new ValidationException(
                        "The null type is reserved for representing untyped NULL literals. It should not be "
                                + "used in constructed types. Please cast NULL literals to a more explicit type.");
            }
        }
    }

    /**
     * Creates a indicator type for processing-time, but with similar properties as SQL timestamp.
     */
    public RelDataType createProctimeIndicatorType(boolean isNullable) {
        final RelDataType originalType =
                createFieldTypeFromLogicalType(new LocalZonedTimestampType(isNullable, 3));
        return canonize(
                new TimeIndicatorRelDataType(
                        getTypeSystem(), (BasicSqlType) originalType, isNullable, false));
    }

    /** Creates a indicator type for event-time, but with similar properties as SQL timestamp. */
    public RelDataType createRowtimeIndicatorType(boolean isNullable, boolean isTimestampLtz) {
        final RelDataType originalType =
                (isTimestampLtz)
                        ? createFieldTypeFromLogicalType(new LocalZonedTimestampType(isNullable, 3))
                        : createFieldTypeFromLogicalType(new TimestampType(isNullable, 3));

        return canonize(
                new TimeIndicatorRelDataType(
                        getTypeSystem(), (BasicSqlType) originalType, isNullable, true));
    }

    public static RowType toLogicalRowType(RelDataType relType) {
        Preconditions.checkArgument(relType.isStruct());
        return RowType.of(
                relType.getFieldList().stream()
                        .map(fieldType -> toLogicalType(fieldType.getType()))
                        .collect(Collectors.toList())
                        .toArray(new LogicalType[0]),
                relType.getFieldNames().toArray(new String[0]));
    }

    public static boolean isTimeIndicatorType(LogicalType logicalType) {
        return logicalType instanceof TimestampType
                        && ((TimestampType) logicalType).getKind() == TimestampKind.ROWTIME
                || logicalType instanceof LocalZonedTimestampType
                        && ((LocalZonedTimestampType) logicalType).getKind()
                                == TimestampKind.PROCTIME;
    }

    public static boolean isTimeIndicatorType(RelDataType relDataType) {
        return relDataType instanceof TimeIndicatorRelDataType;
    }

    public static boolean isRowtimeIndicatorType(RelDataType relDataType) {
        return relDataType instanceof TimeIndicatorRelDataType
                && ((TimeIndicatorRelDataType) relDataType).isEventTime();
    }

    public static boolean isProctimeIndicatorType(RelDataType relDataType) {
        return relDataType instanceof TimeIndicatorRelDataType
                && !((TimeIndicatorRelDataType) relDataType).isEventTime();
    }

    public static boolean isTimestampLtzIndicatorType(RelDataType relDataType) {
        return relDataType instanceof TimeIndicatorRelDataType
                && ((TimeIndicatorRelDataType) relDataType)
                        .getOriginalType()
                        .getSqlTypeName()
                        .equals(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    }

    @Deprecated
    public static boolean isProctimeIndicatorType(TypeInformation<?> typeInfo) {
        return typeInfo instanceof TimeIndicatorTypeInfo
                && !((TimeIndicatorTypeInfo) typeInfo).isEventTime();
    }

    @Deprecated
    public static boolean isRowtimeIndicatorType(TypeInformation<?> typeInfo) {
        return typeInfo instanceof TimeIndicatorTypeInfo
                && ((TimeIndicatorTypeInfo) typeInfo).isEventTime();
    }

    @Deprecated
    public static boolean isTimeIndicatorType(TypeInformation<?> typeInfo) {
        return typeInfo instanceof TimeIndicatorTypeInfo;
    }

    /** Returns a projected {@link RelDataType} of the structure type. */
    public RelDataType projectStructType(RelDataType relType, int[] selectedFields) {
        final List<RelDataTypeField> fields = new ArrayList<>();
        for (int selectedField : selectedFields) {
            fields.add(relType.getFieldList().get(selectedField));
        }
        return this.createStructType(fields);
    }

    /**
     * Creates a struct type with the input fieldNames and input fieldTypes using FlinkTypeFactory.
     *
     * @param tableSchema schema to convert to Calcite's specific one
     * @return a struct type with the input fieldNames, input fieldTypes, and system fields
     */
    public RelDataType buildRelNodeRowType(TableSchema tableSchema) {
        return buildRelNodeRowType(
                tableSchema.getFieldNames(),
                Arrays.stream(tableSchema.getFieldDataTypes())
                        .map(DataType::getLogicalType)
                        .toArray(LogicalType[]::new));
    }

    /**
     * Creates a table row type with the input fieldNames and input fieldTypes using
     * FlinkTypeFactory. Table row type is table schema for Calcite RelNode. See getRowType of
     * {@link RelNode}. Use FULLY_QUALIFIED to let each field must be referenced explicitly.
     */
    public RelDataType buildRelNodeRowType(RowType rowType) {
        final List<RowType.RowField> fields = rowType.getFields();
        return buildStructType(
                fields.stream().map(RowType.RowField::getName).toArray(String[]::new),
                fields.stream().map(RowType.RowField::getType).toArray(LogicalType[]::new),
                StructKind.FULLY_QUALIFIED);
    }

    /**
     * Creates a table row type with the input fieldNames and input fieldTypes using
     * FlinkTypeFactory. Table row type is table schema for Calcite RelNode. See getRowType of
     * {@link RelNode}. Use FULLY_QUALIFIED to let each field must be referenced explicitly.
     *
     * @param fieldNames field names
     * @param fieldTypes field types, every element is Flink's {@link LogicalType}
     * @return a table row type with the input fieldNames, input fieldTypes.
     */
    public RelDataType buildRelNodeRowType(String[] fieldNames, LogicalType[] fieldTypes) {
        return buildStructType(fieldNames, fieldTypes, StructKind.FULLY_QUALIFIED);
    }

    /**
     * Creates a table row type with the given field names and field types. Table row type is table
     * schema for Calcite {@link RelNode}. See {@link RelNode#getRowType()}.
     *
     * <p>It uses {@link StructKind#FULLY_QUALIFIED} to let each field must be referenced
     * explicitly.
     *
     * @param fieldNames field names
     * @param fieldTypes field types, every element is Flink's {@link LogicalType}
     * @return a table row type with the input fieldNames, input fieldTypes.
     */
    public RelDataType buildRelNodeRowType(List<String> fieldNames, List<LogicalType> fieldTypes) {
        return buildStructType(
                fieldNames.toArray(new String[0]),
                fieldTypes.toArray(new LogicalType[0]),
                StructKind.FULLY_QUALIFIED);
    }

    /**
     * Creates a table row type with the input fieldNames and input fieldTypes using
     * FlinkTypeFactory. Table row type is table schema for Calcite RelNode. See getRowType of
     * {@link RelNode}. Use FULLY_QUALIFIED to let each field must be referenced explicitly.
     *
     * @param fieldNames field names
     * @param fieldTypes field types, every element is Flink's {@link LogicalType}
     * @return a table row type with the input fieldNames, input fieldTypes.
     */
    public RelDataType buildRelNodeRowType(Seq<String> fieldNames, Seq<LogicalType> fieldTypes) {
        return buildRelNodeRowType(
                JavaConverters.seqAsJavaList(fieldNames), JavaConverters.seqAsJavaList(fieldTypes));
    }

    public static LogicalType toLogicalType(RelDataType relType) {
        return toLogicalTypeWithoutNullability(relType).copy(relType.isNullable());
    }

    public static TableSchema toTableSchema(RelDataType relDataType) {
        final String[] fieldNames = relDataType.getFieldNames().toArray(new String[0]);
        final DataType[] fieldTypes =
                relDataType.getFieldList().stream()
                        .map(
                                field ->
                                        LogicalTypeDataTypeConverter.fromLogicalTypeToDataType(
                                                FlinkTypeFactory.toLogicalType(field.getType())))
                        .toArray(DataType[]::new);
        return TableSchema.builder().fields(fieldNames, fieldTypes).build();
    }

    /**
     * Creates a struct type with the physical columns using FlinkTypeFactory.
     *
     * @param tableSchema schema to convert to Calcite's specific one
     * @return a struct type with the input fieldNames, input fieldTypes.
     */
    public RelDataType buildPhysicalRelNodeRowType(TableSchema tableSchema) {
        return buildRelNodeRowType(TableSchemaUtils.getPhysicalSchema(tableSchema));
    }

    /**
     * Creates a struct type with the persisted columns using FlinkTypeFactory.
     *
     * @param tableSchema schema to convert to Calcite's specific one
     * @return a struct type with the input fieldsNames, input fieldTypes.
     */
    public RelDataType buildPersistedRelNodeRowType(TableSchema tableSchema) {
        return buildRelNodeRowType(TableSchemaUtils.getPersistedSchema(tableSchema));
    }

    private static LogicalType toLogicalTypeWithoutNullability(RelDataType relDataType) {
        switch (relDataType.getSqlTypeName()) {
            case BOOLEAN:
                return new BooleanType();
            case TINYINT:
                return new TinyIntType();
            case SMALLINT:
                return new SmallIntType();
            case INTEGER:
                return new IntType();
            case BIGINT:
                return new BigIntType();
            case FLOAT:
                return new FloatType();
            case DOUBLE:
                return new DoubleType();
            case CHAR:
                if (relDataType.getPrecision() == 0) {
                    return CharType.ofEmptyLiteral();
                } else {
                    return new CharType(relDataType.getPrecision());
                }
            case VARCHAR:
                if (relDataType.getPrecision() == 0) {
                    return VarCharType.ofEmptyLiteral();
                } else {
                    return new VarCharType(relDataType.getPrecision());
                }
            case BINARY:
                if (relDataType.getPrecision() == 0) {
                    return BinaryType.ofEmptyLiteral();
                } else {
                    return new BinaryType(relDataType.getPrecision());
                }
            case VARBINARY:
                if (relDataType.getPrecision() == 0) {
                    return VarBinaryType.ofEmptyLiteral();
                } else {
                    return new VarBinaryType(relDataType.getPrecision());
                }
            case DECIMAL:
                return new DecimalType(relDataType.getPrecision(), relDataType.getScale());

            case TIMESTAMP:
                if (relDataType instanceof TimeIndicatorRelDataType) {
                    final TimeIndicatorRelDataType indicator =
                            (TimeIndicatorRelDataType) relDataType;
                    if (indicator.isEventTime()) {
                        return new TimestampType(true, TimestampKind.ROWTIME, 3);
                    } else {
                        throw new TableException(
                                "Processing time indicator only supports"
                                        + " LocalZonedTimestampType, but actual is TimestampType."
                                        + " This is a bug in planner, please file an issue.");
                    }
                } else {
                    return new TimestampType(relDataType.getPrecision());
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (relDataType instanceof TimeIndicatorRelDataType) {
                    TimeIndicatorRelDataType indicator = (TimeIndicatorRelDataType) relDataType;
                    if (indicator.isEventTime()) {
                        return new LocalZonedTimestampType(true, TimestampKind.ROWTIME, 3);
                    } else {
                        return new LocalZonedTimestampType(true, TimestampKind.PROCTIME, 3);
                    }
                }
                return new LocalZonedTimestampType(relDataType.getPrecision());

            case DATE:
                return new DateType();
            case TIME:
                return new TimeType(relDataType.getPrecision());

            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
                return DataTypes.INTERVAL(DataTypes.MONTH()).getLogicalType();

            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                if (relDataType.getPrecision() > 3) {
                    throw new TableException(
                            "DAY_INTERVAL_TYPES precision is not supported: "
                                    + relDataType.getPrecision());
                }
                return DataTypes.INTERVAL(DataTypes.SECOND(3)).getLogicalType();

            case NULL:
                return new NullType();

            case SYMBOL:
                return new SymbolType();

            case COLUMN_LIST:
                return new DescriptorType();

            // extract encapsulated Type
            case ANY:
                if (relDataType instanceof GenericRelDataType) {
                    final GenericRelDataType genericRelDataType = (GenericRelDataType) relDataType;
                    return genericRelDataType.genericType();
                } else {
                    throw new TableException("Type is not supported: " + relDataType);
                }

            case ROW:
                if (relDataType instanceof RelRecordType) {
                    return toLogicalRowType(relDataType);
                } else {
                    throw new TableException("Type is not supported: " + relDataType);
                }

            case STRUCTURED:
                if (relDataType instanceof StructuredRelDataType) {
                    return ((StructuredRelDataType) relDataType).getStructuredType();
                } else {
                    throw new TableException("Type is not supported: " + relDataType);
                }

            case MULTISET:
                return new MultisetType(toLogicalType(relDataType.getComponentType()));

            case ARRAY:
                return new ArrayType(toLogicalType(relDataType.getComponentType()));

            case MAP:
                if (relDataType instanceof MapSqlType) {
                    final MapSqlType mapRelDataType = (MapSqlType) relDataType;
                    return new MapType(
                            toLogicalType(mapRelDataType.getKeyType()),
                            toLogicalType(mapRelDataType.getValueType()));
                } else {
                    throw new TableException("Type is not supported: " + relDataType);
                }

            // CURSOR for UDTF case, whose type info will never be used, just a placeholder
            case CURSOR:
                return new TypeInformationRawType(new NothingTypeInfo());

            case VARIANT:
                return new VariantType();

            case OTHER:
                if (relDataType instanceof RawRelDataType) {
                    return ((RawRelDataType) relDataType).getRawType();
                } else if (relDataType instanceof BitmapRelDataType) {
                    return ((BitmapRelDataType) relDataType).getBitmapType();
                } else if (relDataType instanceof GeographyRelDataType) {
                    return ((GeographyRelDataType) relDataType).getGeographyType();
                } else {
                    throw new TableException("Type is not supported: " + relDataType);
                }

            default:
                throw new TableException("Type is not supported: " + relDataType);
        }
    }
}
