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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.utils.CastExecutor;
import org.apache.flink.table.planner.codegen.CodeGenUtils;
import org.apache.flink.table.runtime.generated.CompileUtils;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.planner.codegen.CodeGenUtils.className;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.cast;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.constructorCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.strLiteral;

/**
 * Base class for {@link CastRule} supporting code generation. This base class implements {@link
 * #create(CastRule.Context, LogicalType, LogicalType)} compiling the generated code block into a
 * {@link CastExecutor} implementation.
 *
 * <p>It is suggested to implement {@link CodeGeneratorCastRule} starting from {@link
 * AbstractNullAwareCodeGeneratorCastRule}, which provides nullability checks, or from {@link
 * AbstractExpressionCodeGeneratorCastRule} to generate simple expression casts.
 */
abstract class AbstractCodeGeneratorCastRule<IN, OUT> extends AbstractCastRule<IN, OUT>
        implements CodeGeneratorCastRule<IN, OUT> {

    protected AbstractCodeGeneratorCastRule(CastRulePredicate predicate) {
        super(predicate);
    }

    @SuppressWarnings("unchecked")
    @Override
    public CastExecutor<IN, OUT> create(
            CastRule.Context castRuleContext,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        final String inputTerm = "_myInput";
        final String inputIsNullTerm = "_myInputIsNull";
        final String castExecutorClassName = CodeGenUtils.newName("GeneratedCastExecutor");
        final String inputTypeTerm = CodeGenUtils.boxedTypeTermForType(inputLogicalType);

        final CastExecutorCodeGeneratorContext ctx =
                new CastExecutorCodeGeneratorContext(castRuleContext);
        final CastCodeBlock codeBlock =
                generateCodeBlock(
                        ctx, inputTerm, inputIsNullTerm, inputLogicalType, targetLogicalType);

        // Class fields can contain type serializers
        final String classFieldDecls =
                Stream.concat(
                                ctx.typeSerializers.values().stream()
                                        .map(
                                                entry ->
                                                        "private final "
                                                                + className(
                                                                        entry.getValue().getClass())
                                                                + " "
                                                                + entry.getKey()
                                                                + ";"),
                                ctx.getClassFields().stream())
                        .collect(Collectors.joining("\n"));

        final String constructorSignature =
                "public "
                        + castExecutorClassName
                        + "("
                        + ctx.typeSerializers.values().stream()
                                .map(
                                        entry ->
                                                className(entry.getValue().getClass())
                                                        + " "
                                                        + entry.getKey())
                                .collect(Collectors.joining(", "))
                        + ")";
        final String constructorBody =
                ctx.getDeclaredTypeSerializers().stream()
                        .map(name -> "this." + name + " = " + name + ";\n")
                        .collect(Collectors.joining());

        // Because janino doesn't support generics, we need to manually cast the input variable of
        // the cast method
        final String functionSignature =
                "@Override public Object cast(Object _myInputObj) throws "
                        + className(TableException.class);

        // Write the function body
        final CastRuleUtils.CodeWriter bodyWriter = new CastRuleUtils.CodeWriter();
        bodyWriter.declStmt(inputTypeTerm, inputTerm, cast(inputTypeTerm, "_myInputObj"));
        bodyWriter.declStmt("boolean", inputIsNullTerm, "_myInputObj == null");
        ctx.variableDeclarationStatements.forEach(decl -> bodyWriter.appendBlock(decl + "\n"));

        if (this.canFail()) {
            bodyWriter.tryCatchStmt(
                    tryWriter ->
                            tryWriter.append(codeBlock).stmt("return " + codeBlock.getReturnTerm()),
                    (exceptionTerm, catchWriter) ->
                            catchWriter.throwStmt(
                                    constructorCall(
                                            TableException.class,
                                            strLiteral(
                                                    "Error when casting "
                                                            + inputLogicalType
                                                            + " to "
                                                            + targetLogicalType
                                                            + "."),
                                            exceptionTerm)));
        } else {
            bodyWriter.append(codeBlock).stmt("return " + codeBlock.getReturnTerm());
        }

        final String classCode =
                "public final class "
                        + castExecutorClassName
                        + " implements "
                        + className(CastExecutor.class)
                        + " {\n"
                        + classFieldDecls
                        + "\n"
                        + constructorSignature
                        + " {\n"
                        + constructorBody
                        + "}\n"
                        + functionSignature
                        + " {\n"
                        + bodyWriter
                        + "}\n}";

        try {
            Object[] constructorArgs =
                    ctx.getTypeSerializersInstances().toArray(new TypeSerializer[0]);
            return (CastExecutor<IN, OUT>)
                    CompileUtils.compile(
                                    castRuleContext.getClassLoader(),
                                    castExecutorClassName,
                                    classCode)
                            .getConstructors()[0]
                            .newInstance(constructorArgs);
        } catch (Throwable e) {
            throw new FlinkRuntimeException(
                    "Cast executor cannot be instantiated. This is a bug. Please file an issue. Code:\n"
                            + classCode,
                    e);
        }
    }

    private static final class CastExecutorCodeGeneratorContext
            implements CodeGeneratorCastRule.Context {

        private final CastRule.Context castRuleCtx;

        private final Map<LogicalType, Map.Entry<String, TypeSerializer<?>>> typeSerializers =
                new LinkedHashMap<>();
        private final List<String> variableDeclarationStatements = new ArrayList<>();
        private final List<String> classFields = new ArrayList<>();
        private int variableIndex = 0;

        private CastExecutorCodeGeneratorContext(CastRule.Context castRuleCtx) {
            this.castRuleCtx = castRuleCtx;
        }

        @Override
        public boolean legacyBehaviour() {
            return castRuleCtx.legacyBehaviour();
        }

        @Override
        public String getSessionTimeZoneTerm() {
            return "java.util.TimeZone.getTimeZone(\""
                    + castRuleCtx.getSessionZoneId().getId()
                    + "\")";
        }

        @Override
        public String declareVariable(String type, String variablePrefix) {
            String variableName = variablePrefix + "$" + variableIndex;
            variableDeclarationStatements.add(type + " " + variableName + ";");
            variableIndex++;
            return variableName;
        }

        @Override
        public String declareTypeSerializer(LogicalType type) {
            return typeSerializers
                    .computeIfAbsent(
                            type,
                            t -> {
                                Map.Entry<String, TypeSerializer<?>> e =
                                        new SimpleImmutableEntry<>(
                                                "typeSerializer$" + variableIndex,
                                                InternalSerializers.create(t));
                                variableIndex++;
                                return e;
                            })
                    .getKey();
        }

        @Override
        public String declareClassField(String type, String name, String initialization) {
            this.classFields.add(type + " " + name + " = " + initialization + ";");
            return "this." + name;
        }

        public List<String> getDeclaredTypeSerializers() {
            return this.typeSerializers.values().stream()
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
        }

        public List<TypeSerializer<?>> getTypeSerializersInstances() {
            return this.typeSerializers.values().stream()
                    .map(Map.Entry::getValue)
                    .collect(Collectors.toList());
        }

        public List<String> getClassFields() {
            return classFields;
        }
    }
}
