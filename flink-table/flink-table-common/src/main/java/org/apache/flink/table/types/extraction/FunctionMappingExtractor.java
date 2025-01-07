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

package org.apache.flink.table.types.extraction;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.extraction.ExtractionUtils.Autoboxing;
import org.apache.flink.table.types.extraction.FunctionResultTemplate.FunctionOutputTemplate;
import org.apache.flink.table.types.extraction.FunctionResultTemplate.FunctionStateTemplate;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.apache.flink.table.types.extraction.ExtractionUtils.collectAnnotationsOfClass;
import static org.apache.flink.table.types.extraction.ExtractionUtils.collectAnnotationsOfMethod;
import static org.apache.flink.table.types.extraction.ExtractionUtils.extractionError;
import static org.apache.flink.table.types.extraction.ExtractionUtils.getClassFromType;
import static org.apache.flink.table.types.extraction.ExtractionUtils.isAssignable;
import static org.apache.flink.table.types.extraction.ExtractionUtils.isInvokable;
import static org.apache.flink.table.types.extraction.ExtractionUtils.resolveVariableWithClassContext;

/**
 * Utility for extracting function mappings from signature to result, e.g. from (INT, STRING) to
 * BOOLEAN for {@link UserDefinedFunction}.
 *
 * <p>Both the signature and result can either come from local or global {@link FunctionHint}s, or
 * are extracted reflectively using the implementation methods and/or function generics.
 */
@Internal
final class FunctionMappingExtractor extends BaseMappingExtractor {

    private final Class<? extends UserDefinedFunction> function;

    private final @Nullable ResultExtraction stateExtraction;
    private final @Nullable MethodVerification stateVerification;

    FunctionMappingExtractor(
            DataTypeFactory typeFactory,
            Class<? extends UserDefinedFunction> function,
            String methodName,
            SignatureExtraction signatureExtraction,
            @Nullable ResultExtraction stateExtraction,
            @Nullable MethodVerification stateVerification,
            ResultExtraction outputExtraction,
            @Nullable MethodVerification outputVerification) {
        super(typeFactory, methodName, signatureExtraction, outputExtraction, outputVerification);
        this.function = function;
        this.stateExtraction = stateExtraction;
        this.stateVerification = stateVerification;
    }

    Map<FunctionSignatureTemplate, FunctionStateTemplate> extractStateMapping() {
        Preconditions.checkState(supportsState());
        try {
            return extractResultMappings(
                    stateExtraction, FunctionTemplate::getStateTemplate, stateVerification);
        } catch (Throwable t) {
            throw extractionError(t, "Error in extracting a signature to state mapping.");
        }
    }

    // --------------------------------------------------------------------------------------------
    // Extraction strategies
    // --------------------------------------------------------------------------------------------

    /**
     * Extraction that uses the method return type for producing a {@link FunctionOutputTemplate}.
     */
    static ResultExtraction createOutputFromReturnTypeInMethod() {
        return (extractor, method) -> {
            final DataType dataType =
                    DataTypeExtractor.extractFromMethodReturnType(
                            extractor.typeFactory, extractor.getFunctionClass(), method);
            return FunctionResultTemplate.ofOutput(dataType);
        };
    }

    /**
     * Extraction that uses a generic type variable for producing a {@link FunctionResultTemplate}.
     *
     * <p>If enabled, a {@link DataTypeHint} from method or class has higher priority.
     */
    static ResultExtraction createOutputFromGenericInClass(
            Class<? extends UserDefinedFunction> baseClass,
            int genericPos,
            boolean allowDataTypeHint) {
        return (extractor, method) -> {
            if (allowDataTypeHint) {
                Optional<FunctionResultTemplate> hints = extractHints(extractor, method);
                if (hints.isPresent()) {
                    return hints.get();
                }
            }
            final DataType dataType =
                    DataTypeExtractor.extractFromGeneric(
                            extractor.typeFactory,
                            baseClass,
                            genericPos,
                            extractor.getFunctionClass());
            return FunctionResultTemplate.ofOutput(dataType);
        };
    }

    /**
     * Extraction that uses a generic type variable of a method parameter for producing a {@link
     * FunctionResultTemplate}.
     *
     * <p>If enabled, a {@link DataTypeHint} from method or class has higher priority.
     */
    static ResultExtraction createOutputFromGenericInMethod(
            int paramPos, int genericPos, boolean allowDataTypeHint) {
        return (extractor, method) -> {
            if (allowDataTypeHint) {
                Optional<FunctionResultTemplate> hints = extractHints(extractor, method);
                if (hints.isPresent()) {
                    return hints.get();
                }
            }
            final DataType dataType =
                    DataTypeExtractor.extractFromGenericMethodParameter(
                            extractor.typeFactory,
                            extractor.getFunctionClass(),
                            method,
                            paramPos,
                            genericPos);
            return FunctionResultTemplate.ofOutput(dataType);
        };
    }

    // --------------------------------------------------------------------------------------------
    // Verification strategies
    // --------------------------------------------------------------------------------------------

    /** Verification that checks a method by parameters (arguments only) and return type. */
    static MethodVerification createParameterAndReturnTypeVerification() {
        return (method, state, arguments, result) -> {
            checkNoState(state);
            checkScalarArgumentsOnly(arguments);
            final Class<?>[] parameters = assembleParameters(null, arguments);
            assert result != null;
            final Class<?> resultClass = result.toClass();
            final Class<?> returnType = method.getReturnType();
            // Parameters should be validated using strict autoboxing.
            // For return types, we can be more flexible as the UDF should know what it declared.
            final boolean isValid =
                    isInvokable(Autoboxing.STRICT, method, parameters)
                            && isAssignable(resultClass, returnType, Autoboxing.JVM);
            if (!isValid) {
                throw createMethodNotFoundError(method.getName(), parameters, resultClass, "");
            }
        };
    }

    /** Verification that checks a method by parameters (arguments only or with accumulator). */
    static MethodVerification createParameterVerification(boolean requireAccumulator) {
        return (method, state, arguments, result) -> {
            if (requireAccumulator) {
                checkSingleState(state);
            } else {
                checkNoState(state);
            }
            checkScalarArgumentsOnly(arguments);
            final Class<?>[] parameters = assembleParameters(state, arguments);
            // Parameters should be validated using strict autoboxing.
            if (!isInvokable(Autoboxing.STRICT, method, parameters)) {
                throw createMethodNotFoundError(
                        method.getName(),
                        parameters,
                        null,
                        requireAccumulator ? "(<accumulator> [, <argument>]*)" : "");
            }
        };
    }

    /**
     * Verification that checks a method by parameters (arguments only) with mandatory {@link
     * CompletableFuture}.
     */
    static MethodVerification createParameterAndCompletableFutureVerification(Class<?> baseClass) {
        return (method, state, arguments, result) -> {
            checkNoState(state);
            checkScalarArgumentsOnly(arguments);
            final Class<?>[] parameters = assembleParameters(null, arguments);
            final Class<?>[] parametersWithFuture =
                    Stream.concat(Stream.of(CompletableFuture.class), Arrays.stream(parameters))
                            .toArray(Class<?>[]::new);
            assert result != null;
            final Class<?> resultClass = result.toClass();
            Type genericType = method.getGenericParameterTypes()[0];
            genericType = resolveVariableWithClassContext(baseClass, genericType);
            if (!(genericType instanceof ParameterizedType)) {
                throw extractionError(
                        "The method '%s' needs generic parameters for the CompletableFuture at position %d.",
                        method.getName(), 0);
            }
            final Type returnType = ((ParameterizedType) genericType).getActualTypeArguments()[0];
            Class<?> returnTypeClass = getClassFromType(returnType);
            // Parameters should be validated using strict autoboxing.
            // For return types, we can be more flexible as the UDF should know what it declared.
            if (!(isInvokable(Autoboxing.STRICT, method, parametersWithFuture)
                    && isAssignable(resultClass, returnTypeClass, Autoboxing.JVM))) {
                throw createMethodNotFoundError(
                        method.getName(),
                        parametersWithFuture,
                        null,
                        "(<completable future> [, <argument>]*)");
            }
        };
    }

    /**
     * Verification that checks a method by parameters (state and arguments) with optional context.
     */
    static MethodVerification createParameterAndOptionalContextVerification(
            Class<?> context, boolean allowState) {
        return (method, state, arguments, result) -> {
            if (!allowState) {
                checkNoState(state);
            }
            final Class<?>[] parameters = assembleParameters(state, arguments);
            final Class<?>[] parametersWithContext =
                    Stream.concat(Stream.of(context), Arrays.stream(parameters))
                            .toArray(Class<?>[]::new);
            // Parameters should be validated using strict autoboxing.
            if (!isInvokable(Autoboxing.STRICT, method, parameters)
                    && !isInvokable(Autoboxing.STRICT, method, parametersWithContext)) {
                throw createMethodNotFoundError(
                        method.getName(),
                        parameters,
                        null,
                        allowState ? "(<context>? [, <state>]* [, <argument>]*)" : "");
            }
        };
    }

    // --------------------------------------------------------------------------------------------
    // Methods from super class
    // --------------------------------------------------------------------------------------------

    Class<? extends UserDefinedFunction> getFunction() {
        return function;
    }

    boolean supportsState() {
        return stateExtraction != null;
    }

    @Override
    protected Set<FunctionTemplate> extractGlobalFunctionTemplates() {
        return TemplateUtils.extractGlobalFunctionTemplates(typeFactory, function);
    }

    @Override
    protected Set<FunctionTemplate> extractLocalFunctionTemplates(Method method) {
        return TemplateUtils.extractLocalFunctionTemplates(typeFactory, method);
    }

    @Override
    protected List<Method> collectMethods(String methodName) {
        return ExtractionUtils.collectMethods(function, methodName);
    }

    @Override
    protected Class<?> getFunctionClass() {
        return function;
    }

    @Override
    protected String getHintType() {
        return "Function";
    }

    // --------------------------------------------------------------------------------------------
    // Helper methods
    // --------------------------------------------------------------------------------------------

    /** Uses hints to extract functional template. */
    private static Optional<FunctionResultTemplate> extractHints(
            BaseMappingExtractor extractor, Method method) {
        final Set<DataTypeHint> dataTypeHints = new HashSet<>();
        dataTypeHints.addAll(collectAnnotationsOfMethod(DataTypeHint.class, method));
        dataTypeHints.addAll(
                collectAnnotationsOfClass(DataTypeHint.class, extractor.getFunctionClass()));
        if (dataTypeHints.size() > 1) {
            throw extractionError(
                    "More than one data type hint found for output of function. "
                            + "Please use a function hint instead.");
        }
        if (dataTypeHints.size() == 1) {
            return Optional.ofNullable(
                    FunctionTemplate.createOutputTemplate(
                            extractor.typeFactory, dataTypeHints.iterator().next()));
        }
        // otherwise continue with regular extraction
        return Optional.empty();
    }
}
