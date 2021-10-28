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

package org.apache.flink.table.planner.functions.casting.rules;

import org.apache.flink.table.planner.codegen.CodeGenUtils;
import org.apache.flink.table.planner.functions.casting.CastCodeBlock;
import org.apache.flink.table.planner.functions.casting.CastRule;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.commons.lang3.StringEscapeUtils;

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

    static String functionCall(String functionName, Object... args) {
        return functionName
                + "("
                + Arrays.stream(args).map(Object::toString).collect(Collectors.joining(", "))
                + ")";
    }

    static String functionCall(Method staticMethod, Object... args) {
        return functionCall(CodeGenUtils.qualifyMethod(staticMethod), args);
    }

    static String constructorCall(Class<?> clazz, Object... args) {
        return functionCall("new " + className(clazz), args);
    }

    static String methodCall(String instanceTerm, String methodName, Object... args) {
        return functionCall(instanceTerm + "." + methodName, args);
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

    static String ternaryOperator(String condition, String ifTrue, String ifFalse) {
        return "((" + condition + ") ? (" + ifTrue + ") : (" + ifFalse + "))";
    }

    static String strLiteral() {
        return "\"\"";
    }

    static String strLiteral(String str) {
        return "\"" + StringEscapeUtils.escapeJava(str) + "\"";
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

        public CodeWriter append(CastCodeBlock codeBlock) {
            builder.append(codeBlock.getCode());
            return this;
        }

        public CodeWriter append(String codeBlock) {
            builder.append(codeBlock);
            return this;
        }

        @Override
        public String toString() {
            return builder.toString();
        }
    }
}
