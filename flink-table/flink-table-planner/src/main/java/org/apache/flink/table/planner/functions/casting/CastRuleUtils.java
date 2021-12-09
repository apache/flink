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

package org.apache.flink.table.planner.functions.casting;

import org.apache.flink.table.planner.codegen.CodeGenUtils;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.utils.EncodingUtils;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.codegen.CodeGenUtils.className;
import static org.apache.flink.table.planner.codegen.CodeGenUtils.newName;
import static org.apache.flink.table.planner.codegen.CodeGenUtils.primitiveDefaultValue;
import static org.apache.flink.table.planner.codegen.CodeGenUtils.primitiveTypeTermForType;

/** This class contains a set of utilities to develop {@link CastRule}. */
final class CastRuleUtils {

    static final String NULL_STR_LITERAL = strLiteral("null");
    static final String EMPTY_STR_LITERAL = "\"\"";

    static String staticCall(Class<?> clazz, String methodName, Object... args) {
        return methodCall(className(clazz), methodName, args);
    }

    static String staticCall(Method staticMethod, Object... args) {
        return functionCall(CodeGenUtils.qualifyMethod(staticMethod), args);
    }

    static String constructorCall(Class<?> clazz, Object... args) {
        return functionCall("new " + className(clazz), args);
    }

    static String methodCall(String instanceTerm, String methodName, Object... args) {
        return functionCall(instanceTerm + "." + methodName, args);
    }

    private static String functionCall(String functionName, Object... args) {
        return functionName
                + "("
                + Arrays.stream(args).map(Object::toString).collect(Collectors.joining(", "))
                + ")";
    }

    static String newArray(String innerType, String arraySize) {
        return "new " + innerType + "[" + arraySize + "]";
    }

    static String stringConcat(Object... args) {
        return Arrays.stream(args).map(Object::toString).collect(Collectors.joining(" + "));
    }

    static String accessStaticField(Class<?> clazz, String fieldName) {
        return className(clazz) + "." + fieldName;
    }

    static String arrayLength(String instanceTerm) {
        return instanceTerm + ".length";
    }

    static String ternaryOperator(String condition, String ifTrue, String ifFalse) {
        return "((" + condition + ") ? (" + ifTrue + ") : (" + ifFalse + "))";
    }

    static String strLiteral(String str) {
        return "\"" + EncodingUtils.escapeJava(str) + "\"";
    }

    static String cast(String target, String expression) {
        return "((" + target + ")(" + expression + "))";
    }

    static String castToPrimitive(LogicalType target, String expression) {
        return cast(primitiveTypeTermForType(target), expression);
    }

    static String unbox(String term, LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return methodCall(term, "booleanValue");
            case TINYINT:
                return methodCall(term, "byteValue");
            case SMALLINT:
                return methodCall(term, "shortValue");
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
                return methodCall(term, "intValue");
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return methodCall(term, "longValue");
            case FLOAT:
                return methodCall(term, "floatValue");
            case DOUBLE:
                return methodCall(term, "doubleValue");
            case DISTINCT_TYPE:
                return unbox(term, ((DistinctType) type).getSourceType());
        }
        return term;
    }

    static String box(String term, LogicalType type) {
        switch (type.getTypeRoot()) {
                // ordered by type root definition
            case BOOLEAN:
                return staticCall(Boolean.class, "valueOf", term);
            case TINYINT:
                return staticCall(Byte.class, "valueOf", term);
            case SMALLINT:
                return staticCall(Short.class, "valueOf", term);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
                return staticCall(Integer.class, "valueOf", term);
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return staticCall(Long.class, "valueOf", term);
            case FLOAT:
                return staticCall(Float.class, "valueOf", term);
            case DOUBLE:
                return staticCall(Double.class, "valueOf", term);
            case DISTINCT_TYPE:
                box(term, ((DistinctType) type).getSourceType());
        }
        return term;
    }

    static String binaryWriterWriteField(
            CodeGeneratorCastRule.Context context,
            String writerTerm,
            LogicalType logicalType,
            String indexTerm,
            String fieldValTerm) {
        return CodeGenUtils.binaryWriterWriteField(
                context::declareTypeSerializer,
                String.valueOf(indexTerm),
                fieldValTerm,
                writerTerm,
                logicalType);
    }

    static String binaryWriterWriteNull(
            String writerTerm, LogicalType logicalType, String indexTerm) {
        return CodeGenUtils.binaryWriterWriteNull(indexTerm, writerTerm, logicalType);
    }

    static final class CodeWriter {
        StringBuilder builder = new StringBuilder();

        public CodeWriter declStmt(String varType, String varName, String value) {
            return stmt(varType + " " + varName + " = " + value);
        }

        public CodeWriter declStmt(Class<?> clazz, String varName, String value) {
            return declStmt(className(clazz), varName, value);
        }

        public CodeWriter declPrimitiveStmt(LogicalType logicalType, String varName, String value) {
            return declStmt(primitiveTypeTermForType(logicalType), varName, value);
        }

        public CodeWriter declPrimitiveStmt(LogicalType logicalType, String varName) {
            return declStmt(
                    primitiveTypeTermForType(logicalType),
                    varName,
                    primitiveDefaultValue(logicalType));
        }

        public CodeWriter declStmt(String varType, String varName) {
            return stmt(varType + " " + varName);
        }

        public CodeWriter declStmt(Class<?> clazz, String varName) {
            return declStmt(className(clazz), varName);
        }

        public CodeWriter assignStmt(String varName, String value) {
            return stmt(varName + " = " + value);
        }

        public CodeWriter assignArrayStmt(String varName, String index, String value) {
            return stmt(varName + "[" + index + "] = " + value);
        }

        public CodeWriter stmt(String stmt) {
            builder.append(stmt).append(';').append('\n');
            return this;
        }

        public CodeWriter forStmt(
                String upperBound, BiConsumer<String, CodeWriter> bodyWriterConsumer) {
            final String indexTerm = newName("i");
            final CodeWriter innerWriter = new CodeWriter();

            builder.append("for (int ")
                    .append(indexTerm)
                    .append(" = 0; ")
                    .append(indexTerm)
                    .append(" < ")
                    .append(upperBound)
                    .append("; ")
                    .append(indexTerm)
                    .append("++) {\n");
            bodyWriterConsumer.accept(indexTerm, innerWriter);
            builder.append(innerWriter).append("}\n");

            return this;
        }

        public CodeWriter ifStmt(String condition, Consumer<CodeWriter> bodyWriterConsumer) {
            final CodeWriter innerWriter = new CodeWriter();

            builder.append("if (").append(condition).append(") {\n");
            bodyWriterConsumer.accept(innerWriter);
            builder.append(innerWriter).append("}\n");

            return this;
        }

        public CodeWriter ifStmt(
                String condition,
                Consumer<CodeWriter> thenWriterConsumer,
                Consumer<CodeWriter> elseWriterConsumer) {
            final CodeWriter thenWriter = new CodeWriter();
            final CodeWriter elseWriter = new CodeWriter();

            builder.append("if (").append(condition).append(") {\n");
            thenWriterConsumer.accept(thenWriter);
            builder.append(thenWriter).append("} else {\n");
            elseWriterConsumer.accept(elseWriter);
            builder.append(elseWriter).append("}\n");

            return this;
        }

        public CodeWriter tryCatchStmt(
                Consumer<CodeWriter> bodyWriterConsumer,
                BiConsumer<String, CodeWriter> catchConsumer) {
            return tryCatchStmt(bodyWriterConsumer, Throwable.class, catchConsumer);
        }

        public CodeWriter tryCatchStmt(
                Consumer<CodeWriter> bodyWriterConsumer,
                Class<? extends Throwable> catchClass,
                BiConsumer<String, CodeWriter> catchConsumer) {
            final String exceptionTerm = newName("e");

            final CodeWriter bodyWriter = new CodeWriter();
            final CodeWriter catchWriter = new CodeWriter();

            builder.append("try {\n");
            bodyWriterConsumer.accept(bodyWriter);
            builder.append(bodyWriter)
                    .append("} catch (")
                    .append(className(catchClass))
                    .append(" ")
                    .append(exceptionTerm)
                    .append(") {\n");
            catchConsumer.accept(exceptionTerm, catchWriter);
            builder.append(catchWriter).append("}\n");

            return this;
        }

        public CodeWriter append(CastCodeBlock codeBlock) {
            builder.append(codeBlock.getCode());
            return this;
        }

        public CodeWriter throwStmt(String expression) {
            builder.append("throw ").append(expression).append(";");
            return this;
        }

        public CodeWriter appendBlock(String codeBlock) {
            builder.append(codeBlock);
            return this;
        }

        @Override
        public String toString() {
            return builder.toString();
        }
    }
}
