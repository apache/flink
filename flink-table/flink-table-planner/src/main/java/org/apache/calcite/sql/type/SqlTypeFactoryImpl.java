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
package org.apache.calcite.sql.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.charset.Charset;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Default implementation {@link SqlTypeFactoryImpl}, the class was copied over because of
 * FLINK-31350.
 *
 * <p>FLINK modifications are at lines
 *
 * <ol>
 *   <li>Should be removed after fix of FLINK-31350: Lines 541 ~ 553.
 * </ol>
 */
public class SqlTypeFactoryImpl extends RelDataTypeFactoryImpl {
    // ~ Constructors -----------------------------------------------------------

    public SqlTypeFactoryImpl(RelDataTypeSystem typeSystem) {
        super(typeSystem);
    }

    // ~ Methods ----------------------------------------------------------------

    @Override
    public RelDataType createSqlType(SqlTypeName typeName) {
        if (typeName.allowsPrec()) {
            return createSqlType(typeName, typeSystem.getDefaultPrecision(typeName));
        }
        assertBasic(typeName);
        RelDataType newType = new BasicSqlType(typeSystem, typeName);
        return canonize(newType);
    }

    @Override
    public RelDataType createSqlType(SqlTypeName typeName, int precision) {
        final int maxPrecision = typeSystem.getMaxPrecision(typeName);
        if (maxPrecision >= 0 && precision > maxPrecision) {
            precision = maxPrecision;
        }
        if (typeName.allowsScale()) {
            return createSqlType(typeName, precision, typeName.getDefaultScale());
        }
        assertBasic(typeName);
        assert (precision >= 0) || (precision == RelDataType.PRECISION_NOT_SPECIFIED);
        // Does not check precision when typeName is SqlTypeName#NULL.
        RelDataType newType =
                precision == RelDataType.PRECISION_NOT_SPECIFIED
                        ? new BasicSqlType(typeSystem, typeName)
                        : new BasicSqlType(typeSystem, typeName, precision);
        newType = SqlTypeUtil.addCharsetAndCollation(newType, this);
        return canonize(newType);
    }

    @Override
    public RelDataType createSqlType(SqlTypeName typeName, int precision, int scale) {
        assertBasic(typeName);
        assert (precision >= 0) || (precision == RelDataType.PRECISION_NOT_SPECIFIED);
        final int maxPrecision = typeSystem.getMaxPrecision(typeName);
        if (maxPrecision >= 0 && precision > maxPrecision) {
            precision = maxPrecision;
        }
        RelDataType newType = new BasicSqlType(typeSystem, typeName, precision, scale);
        newType = SqlTypeUtil.addCharsetAndCollation(newType, this);
        return canonize(newType);
    }

    @Override
    public RelDataType createUnknownType() {
        // FLINK MODIFICATION BEGIN
        return canonize(new UnknownSqlType(this));
        // FLINK MODIFICATION END
    }

    @Override
    public RelDataType createMultisetType(RelDataType type, long maxCardinality) {
        assert maxCardinality == -1;
        RelDataType newType = new MultisetSqlType(type, false);
        return canonize(newType);
    }

    @Override
    public RelDataType createArrayType(RelDataType elementType, long maxCardinality) {
        assert maxCardinality == -1;
        ArraySqlType newType = new ArraySqlType(elementType, false);
        return canonize(newType);
    }

    @Override
    public RelDataType createMapType(RelDataType keyType, RelDataType valueType) {
        MapSqlType newType = new MapSqlType(keyType, valueType, false);
        return canonize(newType);
    }

    @Override
    public RelDataType createSqlIntervalType(SqlIntervalQualifier intervalQualifier) {
        RelDataType newType = new IntervalSqlType(typeSystem, intervalQualifier, false);
        return canonize(newType);
    }

    @Override
    public RelDataType createTypeWithCharsetAndCollation(
            RelDataType type, Charset charset, SqlCollation collation) {
        assert SqlTypeUtil.inCharFamily(type) : type;
        requireNonNull(charset, "charset");
        requireNonNull(collation, "collation");
        RelDataType newType;
        if (type instanceof BasicSqlType) {
            BasicSqlType sqlType = (BasicSqlType) type;
            newType = sqlType.createWithCharsetAndCollation(charset, collation);
        } else if (type instanceof JavaType) {
            JavaType javaType = (JavaType) type;
            newType =
                    new JavaType(
                            javaType.getJavaClass(), javaType.isNullable(), charset, collation);
        } else {
            throw Util.needToImplement("need to implement " + type);
        }
        return canonize(newType);
    }

    @Override
    public @Nullable RelDataType leastRestrictive(List<RelDataType> types) {
        assert types != null;
        assert types.size() >= 1;

        RelDataType type0 = types.get(0);
        if (type0.getSqlTypeName() != null) {
            RelDataType resultType = leastRestrictiveSqlType(types);
            if (resultType != null) {
                return resultType;
            }
            return leastRestrictiveByCast(types);
        }

        return super.leastRestrictive(types);
    }

    private @Nullable RelDataType leastRestrictiveByCast(List<RelDataType> types) {
        RelDataType resultType = types.get(0);
        boolean anyNullable = resultType.isNullable();
        for (int i = 1; i < types.size(); i++) {
            RelDataType type = types.get(i);
            if (type.getSqlTypeName() == SqlTypeName.NULL) {
                anyNullable = true;
                continue;
            }

            if (type.isNullable()) {
                anyNullable = true;
            }

            if (SqlTypeUtil.canCastFrom(type, resultType, false)) {
                resultType = type;
            } else {
                if (!SqlTypeUtil.canCastFrom(resultType, type, false)) {
                    return null;
                }
            }
        }
        if (anyNullable) {
            return createTypeWithNullability(resultType, true);
        } else {
            return resultType;
        }
    }

    @Override
    public RelDataType createTypeWithNullability(final RelDataType type, final boolean nullable) {
        final RelDataType newType;
        if (type instanceof BasicSqlType) {
            newType = ((BasicSqlType) type).createWithNullability(nullable);
        } else if (type instanceof MapSqlType) {
            newType = copyMapType(type, nullable);
        } else if (type instanceof ArraySqlType) {
            newType = copyArrayType(type, nullable);
        } else if (type instanceof MultisetSqlType) {
            newType = copyMultisetType(type, nullable);
        } else if (type instanceof IntervalSqlType) {
            newType = copyIntervalType(type, nullable);
        } else if (type instanceof ObjectSqlType) {
            newType = copyObjectType(type, nullable);
        } else {
            return super.createTypeWithNullability(type, nullable);
        }
        return canonize(newType);
    }

    private static void assertBasic(SqlTypeName typeName) {
        assert typeName != null;
        assert typeName != SqlTypeName.MULTISET : "use createMultisetType() instead";
        assert typeName != SqlTypeName.ARRAY : "use createArrayType() instead";
        assert typeName != SqlTypeName.MAP : "use createMapType() instead";
        assert typeName != SqlTypeName.ROW : "use createStructType() instead";
        assert !SqlTypeName.INTERVAL_TYPES.contains(typeName)
                : "use createSqlIntervalType() instead";
    }

    private @Nullable RelDataType leastRestrictiveSqlType(List<RelDataType> types) {
        RelDataType resultType = null;
        int nullCount = 0;
        int nullableCount = 0;
        int javaCount = 0;
        int anyCount = 0;

        for (RelDataType type : types) {
            final SqlTypeName typeName = type.getSqlTypeName();
            if (typeName == null) {
                return null;
            }
            if (typeName == SqlTypeName.ANY) {
                anyCount++;
            }
            if (type.isNullable()) {
                ++nullableCount;
            }
            if (typeName == SqlTypeName.NULL) {
                ++nullCount;
            }
            if (isJavaType(type)) {
                ++javaCount;
            }
        }

        //  if any of the inputs are ANY, the output is ANY
        if (anyCount > 0) {
            return createTypeWithNullability(
                    createSqlType(SqlTypeName.ANY), nullCount > 0 || nullableCount > 0);
        }

        for (int i = 0; i < types.size(); ++i) {
            RelDataType type = types.get(i);
            RelDataTypeFamily family = type.getFamily();

            final SqlTypeName typeName = type.getSqlTypeName();
            if (typeName == SqlTypeName.NULL) {
                continue;
            }

            // Convert Java types; for instance, JavaType(int) becomes INTEGER.
            // Except if all types are either NULL or Java types.
            if (isJavaType(type) && javaCount + nullCount < types.size()) {
                final RelDataType originalType = type;
                type =
                        typeName.allowsPrecScale(true, true)
                                ? createSqlType(typeName, type.getPrecision(), type.getScale())
                                : typeName.allowsPrecScale(true, false)
                                        ? createSqlType(typeName, type.getPrecision())
                                        : createSqlType(typeName);
                type = createTypeWithNullability(type, originalType.isNullable());
            }

            if (resultType == null) {
                resultType = type;
                SqlTypeName sqlTypeName = resultType.getSqlTypeName();
                if (sqlTypeName == SqlTypeName.ROW) {
                    return leastRestrictiveStructuredType(types);
                }
                if (sqlTypeName == SqlTypeName.ARRAY || sqlTypeName == SqlTypeName.MULTISET) {
                    return leastRestrictiveArrayMultisetType(types, sqlTypeName);
                }
                if (sqlTypeName == SqlTypeName.MAP) {
                    return leastRestrictiveMapType(types, sqlTypeName);
                }
            }

            RelDataTypeFamily resultFamily = resultType.getFamily();
            SqlTypeName resultTypeName = resultType.getSqlTypeName();

            if (resultFamily != family) {
                return null;
            }
            if (SqlTypeUtil.inCharOrBinaryFamilies(type)) {
                Charset charset1 = type.getCharset();
                Charset charset2 = resultType.getCharset();
                SqlCollation collation1 = type.getCollation();
                SqlCollation collation2 = resultType.getCollation();

                final int precision =
                        SqlTypeUtil.maxPrecision(resultType.getPrecision(), type.getPrecision());

                // If either type is LOB, then result is LOB with no precision.
                // Otherwise, if either is variable width, result is variable
                // width.  Otherwise, result is fixed width.
                if (SqlTypeUtil.isLob(resultType)) {
                    resultType = createSqlType(resultType.getSqlTypeName());
                } else if (SqlTypeUtil.isLob(type)) {
                    resultType = createSqlType(type.getSqlTypeName());
                } else if (SqlTypeUtil.isBoundedVariableWidth(resultType)) {
                    resultType = createSqlType(resultType.getSqlTypeName(), precision);
                } else {
                    // this catch-all case covers type variable, and both fixed

                    SqlTypeName newTypeName = type.getSqlTypeName();

                    if (typeSystem.shouldConvertRaggedUnionTypesToVarying()) {
                        if (resultType.getPrecision() != type.getPrecision()) {
                            if (newTypeName == SqlTypeName.CHAR) {
                                newTypeName = SqlTypeName.VARCHAR;
                            } else if (newTypeName == SqlTypeName.BINARY) {
                                newTypeName = SqlTypeName.VARBINARY;
                            }
                        }
                    }

                    resultType = createSqlType(newTypeName, precision);
                }
                Charset charset = null;
                // TODO:  refine collation combination rules
                SqlCollation collation0 =
                        collation1 != null && collation2 != null
                                ? SqlCollation.getCoercibilityDyadicOperator(collation1, collation2)
                                : null;
                SqlCollation collation = null;
                if ((charset1 != null) || (charset2 != null)) {
                    if (charset1 == null) {
                        charset = charset2;
                        collation = collation2;
                    } else if (charset2 == null) {
                        charset = charset1;
                        collation = collation1;
                    } else if (charset1.equals(charset2)) {
                        charset = charset1;
                        collation = collation1;
                    } else if (charset1.contains(charset2)) {
                        charset = charset1;
                        collation = collation1;
                    } else {
                        charset = charset2;
                        collation = collation2;
                    }
                }
                if (charset != null) {
                    resultType =
                            createTypeWithCharsetAndCollation(
                                    resultType,
                                    charset,
                                    collation0 != null
                                            ? collation0
                                            : requireNonNull(collation, "collation"));
                }
            } else if (SqlTypeUtil.isExactNumeric(type)) {
                if (SqlTypeUtil.isExactNumeric(resultType)) {
                    // TODO: come up with a cleaner way to support
                    // interval + datetime = datetime
                    if (types.size() > (i + 1)) {
                        RelDataType type1 = types.get(i + 1);
                        if (SqlTypeUtil.isDatetime(type1)) {
                            resultType = type1;
                            return createTypeWithNullability(
                                    resultType, nullCount > 0 || nullableCount > 0);
                        }
                    }
                    if (!type.equals(resultType)) {
                        if (!typeName.allowsPrec() && !resultTypeName.allowsPrec()) {
                            // use the bigger primitive
                            if (type.getPrecision() > resultType.getPrecision()) {
                                resultType = type;
                            }
                        } else {
                            // Let the result type have precision (p), scale (s)
                            // and number of whole digits (d) as follows: d =
                            // max(p1 - s1, p2 - s2) s <= max(s1, s2) p = s + d

                            int p1 = resultType.getPrecision();
                            int p2 = type.getPrecision();
                            int s1 = resultType.getScale();
                            int s2 = type.getScale();
                            final int maxPrecision = typeSystem.getMaxNumericPrecision();
                            final int maxScale = typeSystem.getMaxNumericScale();

                            int dout = Math.max(p1 - s1, p2 - s2);
                            dout = Math.min(dout, maxPrecision);

                            int scale = Math.max(s1, s2);
                            scale = Math.min(scale, maxPrecision - dout);
                            scale = Math.min(scale, maxScale);

                            int precision = dout + scale;
                            assert precision <= maxPrecision;
                            assert precision > 0
                                    || (resultType.getSqlTypeName() == SqlTypeName.DECIMAL
                                            && precision == 0
                                            && scale == 0);

                            resultType = createSqlType(SqlTypeName.DECIMAL, precision, scale);
                        }
                    }
                } else if (SqlTypeUtil.isApproximateNumeric(resultType)) {
                    // already approximate; promote to double just in case
                    // TODO:  only promote when required
                    if (SqlTypeUtil.isDecimal(type)) {
                        // Only promote to double for decimal types
                        resultType = createDoublePrecisionType();
                    }
                } else {
                    return null;
                }
            } else if (SqlTypeUtil.isApproximateNumeric(type)) {
                if (SqlTypeUtil.isApproximateNumeric(resultType)) {
                    if (type.getPrecision() > resultType.getPrecision()) {
                        resultType = type;
                    }
                } else if (SqlTypeUtil.isExactNumeric(resultType)) {
                    if (SqlTypeUtil.isDecimal(resultType)) {
                        resultType = createDoublePrecisionType();
                    } else {
                        resultType = type;
                    }
                } else {
                    return null;
                }
            } else if (SqlTypeUtil.isInterval(type)) {
                // TODO: come up with a cleaner way to support
                // interval + datetime = datetime
                if (types.size() > (i + 1)) {
                    RelDataType type1 = types.get(i + 1);
                    if (SqlTypeUtil.isDatetime(type1)) {
                        resultType = type1;
                        return createTypeWithNullability(
                                resultType, nullCount > 0 || nullableCount > 0);
                    }
                }

                if (!type.equals(resultType)) {
                    // TODO jvs 4-June-2005:  This shouldn't be necessary;
                    // move logic into IntervalSqlType.combine
                    Object type1 = resultType;
                    resultType =
                            ((IntervalSqlType) resultType).combine(this, (IntervalSqlType) type);
                    resultType =
                            ((IntervalSqlType) resultType).combine(this, (IntervalSqlType) type1);
                }
            } else if (SqlTypeUtil.isDatetime(type)) {
                // TODO: come up with a cleaner way to support
                // datetime +/- interval (or integer) = datetime
                if (types.size() > (i + 1)) {
                    RelDataType type1 = types.get(i + 1);
                    if (SqlTypeUtil.isInterval(type1) || SqlTypeUtil.isIntType(type1)) {
                        resultType = type;
                        return createTypeWithNullability(
                                resultType, nullCount > 0 || nullableCount > 0);
                    }
                }
            } else {
                // TODO:  datetime precision details; for now we let
                // leastRestrictiveByCast handle it
                return null;
            }
        }
        if (resultType != null && nullableCount > 0) {
            resultType = createTypeWithNullability(resultType, true);
        }
        return resultType;
    }

    private RelDataType createDoublePrecisionType() {
        return createSqlType(SqlTypeName.DOUBLE);
    }

    private RelDataType copyMultisetType(RelDataType type, boolean nullable) {
        MultisetSqlType mt = (MultisetSqlType) type;
        RelDataType elementType = copyType(mt.getComponentType());
        return new MultisetSqlType(elementType, nullable);
    }

    private RelDataType copyIntervalType(RelDataType type, boolean nullable) {
        return new IntervalSqlType(
                typeSystem,
                requireNonNull(
                        type.getIntervalQualifier(),
                        () -> "type.getIntervalQualifier() for " + type),
                nullable);
    }

    private static RelDataType copyObjectType(RelDataType type, boolean nullable) {
        return new ObjectSqlType(
                type.getSqlTypeName(),
                type.getSqlIdentifier(),
                nullable,
                type.getFieldList(),
                type.getComparability());
    }

    private RelDataType copyArrayType(RelDataType type, boolean nullable) {
        ArraySqlType at = (ArraySqlType) type;
        RelDataType elementType = copyType(at.getComponentType());
        return new ArraySqlType(elementType, nullable);
    }

    private RelDataType copyMapType(RelDataType type, boolean nullable) {
        MapSqlType mt = (MapSqlType) type;
        RelDataType keyType = copyType(mt.getKeyType());
        RelDataType valueType = copyType(mt.getValueType());
        return new MapSqlType(keyType, valueType, nullable);
    }

    @Override
    protected RelDataType canonize(RelDataType type) {
        type = super.canonize(type);
        if (!(type instanceof ObjectSqlType)) {
            return type;
        }
        ObjectSqlType objectType = (ObjectSqlType) type;
        if (!objectType.isNullable()) {
            objectType.setFamily(objectType);
        } else {
            objectType.setFamily((RelDataTypeFamily) createTypeWithNullability(objectType, false));
        }
        return type;
    }

    // FLINK MODIFICATION BEGIN
    /** The unknown type. Similar to the NULL type, but is only equal to itself. */
    static class UnknownSqlType extends BasicSqlType {
        UnknownSqlType(RelDataTypeFactory typeFactory) {
            super(typeFactory.getTypeSystem(), SqlTypeName.NULL);
        }

        @Override
        protected void generateTypeString(StringBuilder sb, boolean withDetail) {
            sb.append("UNKNOWN");
        }
    }
    // FLINK MODIFICATION END
}
