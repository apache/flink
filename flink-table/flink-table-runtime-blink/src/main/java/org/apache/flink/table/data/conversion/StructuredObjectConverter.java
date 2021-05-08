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

package org.apache.flink.table.data.conversion;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.CompileUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.StructuredType;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.extraction.ExtractionUtils.getStructuredField;
import static org.apache.flink.table.types.extraction.ExtractionUtils.getStructuredFieldGetter;
import static org.apache.flink.table.types.extraction.ExtractionUtils.getStructuredFieldSetter;
import static org.apache.flink.table.types.extraction.ExtractionUtils.hasInvokableConstructor;
import static org.apache.flink.table.types.extraction.ExtractionUtils.isStructuredFieldDirectlyReadable;
import static org.apache.flink.table.types.extraction.ExtractionUtils.isStructuredFieldDirectlyWritable;
import static org.apache.flink.table.types.extraction.ExtractionUtils.primitiveToWrapper;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getFieldNames;

/** Converter for {@link StructuredType} of its implementation class. */
@Internal
@SuppressWarnings("unchecked")
public class StructuredObjectConverter<T> implements DataStructureConverter<RowData, T> {

    private static final long serialVersionUID = 1L;

    private final DataStructureConverter<Object, Object>[] fieldConverters;

    private final RowData.FieldGetter[] fieldGetters;

    private final String generatedName;

    private final String generatedCode;

    private transient DataStructureConverter<RowData, T> generatedConverter;

    private StructuredObjectConverter(
            DataStructureConverter<Object, Object>[] fieldConverters,
            RowData.FieldGetter[] fieldGetters,
            String generatedName,
            String generatedCode) {
        this.fieldConverters = fieldConverters;
        this.fieldGetters = fieldGetters;
        this.generatedName = generatedName;
        this.generatedCode = generatedCode;
    }

    @Override
    public void open(ClassLoader classLoader) {
        for (DataStructureConverter<Object, Object> fieldConverter : fieldConverters) {
            fieldConverter.open(classLoader);
        }
        try {
            final Class<?> compiledConverter =
                    CompileUtils.compile(classLoader, generatedName, generatedCode);
            generatedConverter =
                    (DataStructureConverter<RowData, T>)
                            compiledConverter
                                    .getConstructor(
                                            RowData.FieldGetter[].class,
                                            DataStructureConverter[].class)
                                    .newInstance(fieldGetters, fieldConverters);
        } catch (Throwable t) {
            throw new TableException("Error while generating structured type converter.", t);
        }
        generatedConverter.open(classLoader);
    }

    @Override
    public RowData toInternal(T external) {
        return generatedConverter.toInternal(external);
    }

    @Override
    public T toExternal(RowData internal) {
        return generatedConverter.toExternal(internal);
    }

    // --------------------------------------------------------------------------------------------
    // Factory method
    // --------------------------------------------------------------------------------------------

    private static final AtomicInteger nextUniqueClassId = new AtomicInteger();

    public static StructuredObjectConverter<?> create(DataType dataType) {
        try {
            return createOrError(dataType);
        } catch (Throwable t) {
            throw new TableException(
                    String.format("Could not create converter for structured type '%s'.", dataType),
                    t);
        }
    }

    /**
     * Creates a {@link DataStructureConverter} for the given structured type.
     *
     * <p>Note: We do not perform validation if data type and structured type implementation match.
     * This must have been done earlier in the {@link DataTypeFactory}.
     */
    @SuppressWarnings("RedundantCast")
    private static StructuredObjectConverter<?> createOrError(DataType dataType) {
        final List<DataType> fields = dataType.getChildren();

        final DataStructureConverter<Object, Object>[] fieldConverters =
                fields.stream()
                        .map(
                                dt ->
                                        (DataStructureConverter<Object, Object>)
                                                DataStructureConverters.getConverter(dt))
                        .toArray(DataStructureConverter[]::new);

        final RowData.FieldGetter[] fieldGetters =
                IntStream.range(0, fields.size())
                        .mapToObj(
                                pos ->
                                        RowData.createFieldGetter(
                                                fields.get(pos).getLogicalType(), pos))
                        .toArray(RowData.FieldGetter[]::new);

        final Class<?>[] fieldClasses =
                fields.stream().map(DataType::getConversionClass).toArray(Class[]::new);

        final StructuredType structuredType = (StructuredType) dataType.getLogicalType();

        final Class<?> implementationClass =
                structuredType.getImplementationClass().orElseThrow(IllegalStateException::new);

        final int uniqueClassId = nextUniqueClassId.getAndIncrement();

        final String converterName =
                String.format(
                        "%s$%s$Converter",
                        implementationClass.getName().replace('.', '$'), uniqueClassId);
        final String converterCode =
                generateCode(
                        converterName,
                        implementationClass,
                        getFieldNames(structuredType).toArray(new String[0]),
                        fieldClasses);

        return new StructuredObjectConverter<>(
                fieldConverters, fieldGetters, converterName, converterCode);
    }

    private static String generateCode(
            String converterName, Class<?> clazz, String[] fieldNames, Class<?>[] fieldClasses) {
        final int fieldCount = fieldClasses.length;
        final StringBuilder sb = new StringBuilder();

        // we ignore checkstyle here for readability and preserving indention

        line(
                sb,
                "public class ",
                converterName,
                " implements ",
                DataStructureConverter.class,
                " {");
        line(sb, "    private final ", RowData.FieldGetter.class, "[] fieldGetters;");
        line(sb, "    private final ", DataStructureConverter.class, "[] fieldConverters;");

        line(
                sb,
                "    public ",
                converterName,
                "(",
                RowData.FieldGetter.class,
                "[] fieldGetters, ",
                DataStructureConverter.class,
                "[] fieldConverters) {");
        line(sb, "        this.fieldGetters = fieldGetters;");
        line(sb, "        this.fieldConverters = fieldConverters;");
        line(sb, "    }");

        line(sb, "    public ", Object.class, " toInternal(", Object.class, " o) {");
        line(sb, "        final ", clazz, " external = (", clazz, ") o;");
        line(
                sb,
                "        final ",
                GenericRowData.class,
                " genericRow = new ",
                GenericRowData.class,
                "(",
                fieldCount,
                ");");
        for (int pos = 0; pos < fieldCount; pos++) {
            line(sb, "        ", getterExpr(clazz, pos, fieldNames[pos], fieldClasses[pos]), ";");
        }
        line(sb, "        return genericRow;");
        line(sb, "    }");

        line(sb, "    public ", Object.class, " toExternal(", Object.class, " o) {");
        line(sb, "        final ", RowData.class, " internal = (", RowData.class, ") o;");
        if (hasInvokableConstructor(clazz, fieldClasses)) {
            line(sb, "        final ", clazz, " structured = new ", clazz, "(");
            for (int pos = 0; pos < fieldCount; pos++) {
                line(
                        sb,
                        "            ",
                        parameterExpr(pos, fieldClasses[pos]),
                        (pos < fieldCount - 1) ? ", " : "");
            }
            line(sb, "        );");
        } else {
            line(sb, "        final ", clazz, " structured = new ", clazz, "();");
            for (int pos = 0; pos < fieldCount; pos++) {
                line(sb, "        ", setterExpr(clazz, pos, fieldNames[pos]), ";");
            }
        }
        line(sb, "        return structured;");
        line(sb, "    }");
        line(sb, "}");
        return sb.toString();
    }

    private static String getterExpr(
            Class<?> implementationClass, int pos, String fieldName, Class<?> fieldClass) {
        final Field field = getStructuredField(implementationClass, fieldName);
        String accessExpr;
        if (isStructuredFieldDirectlyReadable(field)) {
            // field is accessible without getter
            accessExpr = expr("external.", field.getName());
        } else {
            // field is accessible with a getter
            final Method getter =
                    getStructuredFieldGetter(implementationClass, field)
                            .orElseThrow(IllegalStateException::new);
            accessExpr = expr("external.", getter.getName(), "()");
        }
        accessExpr = castExpr(accessExpr, fieldClass);
        return expr(
                "genericRow.setField(",
                pos,
                ", fieldConverters[",
                pos,
                "].toInternalOrNull(",
                accessExpr,
                "))");
    }

    private static String parameterExpr(int pos, Class<?> fieldClass) {
        final String conversionExpr =
                expr(
                        "fieldConverters[",
                        pos,
                        "].toExternalOrNull(fieldGetters[",
                        pos,
                        "].getFieldOrNull(internal))");
        return castExpr(conversionExpr, fieldClass);
    }

    private static String setterExpr(Class<?> implementationClass, int pos, String fieldName) {
        final Field field = getStructuredField(implementationClass, fieldName);
        final String conversionExpr =
                expr(
                        "fieldConverters[",
                        pos,
                        "].toExternalOrNull(fieldGetters[",
                        pos,
                        "].getFieldOrNull(internal))");
        if (isStructuredFieldDirectlyWritable(field)) {
            // field is accessible without setter
            return expr(
                    "structured.",
                    field.getName(),
                    " = ",
                    castExpr(conversionExpr, field.getType()));
        } else {
            // field is accessible with a setter
            final Method setter =
                    getStructuredFieldSetter(implementationClass, field)
                            .orElseThrow(IllegalStateException::new);
            return expr(
                    "structured.",
                    setter.getName(),
                    "(",
                    castExpr(conversionExpr, setter.getParameterTypes()[0]),
                    ")");
        }
    }

    private static String castExpr(String expr, Class<?> clazz) {
        // help Janino to box primitive types and fix missing generics
        return expr("((", primitiveToWrapper(clazz), ") ", expr, ")");
    }

    private static String expr(Object... parts) {
        final StringBuilder sb = new StringBuilder();
        for (Object part : parts) {
            if (part instanceof Class) {
                sb.append(((Class<?>) part).getCanonicalName());
            } else {
                sb.append(part);
            }
        }
        return sb.toString();
    }

    private static void line(StringBuilder sb, Object... parts) {
        for (Object part : parts) {
            if (part instanceof Class) {
                sb.append(((Class<?>) part).getCanonicalName());
            } else {
                sb.append(part);
            }
        }
        sb.append("\n");
    }
}
