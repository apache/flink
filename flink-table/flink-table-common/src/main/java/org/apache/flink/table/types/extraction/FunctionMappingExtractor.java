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
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.types.extraction.ExtractionUtils.collectAnnotationsOfClass;
import static org.apache.flink.table.types.extraction.ExtractionUtils.collectAnnotationsOfMethod;
import static org.apache.flink.table.types.extraction.ExtractionUtils.extractionError;
import static org.apache.flink.table.types.extraction.ExtractionUtils.isAssignable;
import static org.apache.flink.table.types.extraction.ExtractionUtils.isInvokable;

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

    private final @Nullable ResultExtraction accumulatorExtraction;

    FunctionMappingExtractor(
            DataTypeFactory typeFactory,
            Class<? extends UserDefinedFunction> function,
            String methodName,
            SignatureExtraction signatureExtraction,
            @Nullable ResultExtraction accumulatorExtraction,
            ResultExtraction outputExtraction,
            MethodVerification verification) {
        super(typeFactory, methodName, signatureExtraction, outputExtraction, verification);
        this.function = function;
        this.accumulatorExtraction = accumulatorExtraction;
    }

    Class<? extends UserDefinedFunction> getFunction() {
        return function;
    }

    boolean hasAccumulator() {
        return accumulatorExtraction != null;
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

    Map<FunctionSignatureTemplate, FunctionResultTemplate> extractAccumulatorMapping() {
        Preconditions.checkState(hasAccumulator());
        try {
            return extractResultMappings(
                    accumulatorExtraction,
                    FunctionTemplate::getAccumulatorTemplate,
                    (method, signature, result) -> {
                        // put the result into the signature for accumulators
                        final List<Class<?>> arguments =
                                Stream.concat(Stream.of(result), signature.stream())
                                        .collect(Collectors.toList());
                        verification.verify(method, arguments, null);
                    });
        } catch (Throwable t) {
            throw extractionError(t, "Error in extracting a signature to accumulator mapping.");
        }
    }

    /**
     * Extraction that uses the method return type for producing a {@link FunctionResultTemplate}.
     */
    static ResultExtraction createReturnTypeResultExtraction() {
        return (extractor, method) -> {
            final DataType dataType =
                    DataTypeExtractor.extractFromMethodOutput(
                            extractor.typeFactory, extractor.getFunctionClass(), method);
            return FunctionResultTemplate.of(dataType);
        };
    }

    /**
     * Extraction that uses a generic type variable for producing a {@link FunctionResultTemplate}.
     *
     * <p>If enabled, a {@link DataTypeHint} from method or class has higher priority.
     */
    static ResultExtraction createGenericResultExtraction(
            Class<? extends UserDefinedFunction> baseClass,
            int genericPos,
            boolean allowDataTypeHint) {
        return (extractor, method) -> {
            if (allowDataTypeHint) {
                final Set<DataTypeHint> dataTypeHints = new HashSet<>();
                dataTypeHints.addAll(collectAnnotationsOfMethod(DataTypeHint.class, method));
                dataTypeHints.addAll(
                        collectAnnotationsOfClass(
                                DataTypeHint.class, extractor.getFunctionClass()));
                if (dataTypeHints.size() > 1) {
                    throw extractionError(
                            "More than one data type hint found for output of function. "
                                    + "Please use a function hint instead.");
                }
                if (dataTypeHints.size() == 1) {
                    return FunctionTemplate.createResultTemplate(
                            extractor.typeFactory, dataTypeHints.iterator().next());
                }
                // otherwise continue with regular extraction
            }
            final DataType dataType =
                    DataTypeExtractor.extractFromGeneric(
                            extractor.typeFactory,
                            baseClass,
                            genericPos,
                            extractor.getFunctionClass());
            return FunctionResultTemplate.of(dataType);
        };
    }

    /** Verification that checks a method by parameters and return type. */
    static MethodVerification createParameterAndReturnTypeVerification() {
        return (method, signature, result) -> {
            final Class<?>[] parameters = signature.toArray(new Class[0]);
            final Class<?> returnType = method.getReturnType();
            final boolean isValid =
                    isInvokable(method, parameters) && isAssignable(result, returnType, true);
            if (!isValid) {
                throw createMethodNotFoundError(method.getName(), parameters, result);
            }
        };
    }

    /** Verification that checks a method by parameters including an accumulator. */
    static MethodVerification createParameterWithAccumulatorVerification() {
        return (method, signature, result) -> {
            if (result != null) {
                // ignore the accumulator in the first argument
                createParameterWithArgumentVerification(null).verify(method, signature, result);
            } else {
                // check the signature only
                createParameterVerification().verify(method, signature, null);
            }
        };
    }

    /** Verification that checks a method by parameters including an additional first parameter. */
    static MethodVerification createParameterWithArgumentVerification(
            @Nullable Class<?> argumentClass) {
        return (method, signature, result) -> {
            final Class<?>[] parameters =
                    Stream.concat(Stream.of(argumentClass), signature.stream())
                            .toArray(Class<?>[]::new);
            if (!isInvokable(method, parameters)) {
                throw createMethodNotFoundError(method.getName(), parameters, null);
            }
        };
    }

    /** Verification that checks a method by parameters. */
    static MethodVerification createParameterVerification() {
        return (method, signature, result) -> {
            final Class<?>[] parameters = signature.toArray(new Class[0]);
            if (!isInvokable(method, parameters)) {
                throw createMethodNotFoundError(method.getName(), parameters, null);
            }
        };
    }
}
