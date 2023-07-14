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
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.procedures.Procedure;
import org.apache.flink.table.types.DataType;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.flink.table.types.extraction.ExtractionUtils.isAssignable;
import static org.apache.flink.table.types.extraction.ExtractionUtils.isInvokable;

/**
 * Utility for extracting function mappings from signature to result, e.g. from (INT, STRING) to
 * BOOLEAN for {@link Procedure}.
 *
 * <p>Both the signature and result can either come from local or global {@link
 * org.apache.flink.table.annotation.ProcedureHint}s, or are extracted reflectively using the
 * implementation methods and/or function generics.
 */
@Internal
final class ProcedureMappingExtractor extends BaseMappingExtractor {
    private final Class<? extends Procedure> procedure;

    ProcedureMappingExtractor(
            DataTypeFactory typeFactory,
            Class<? extends Procedure> procedure,
            String methodName,
            SignatureExtraction signatureExtraction,
            ResultExtraction outputExtraction,
            MethodVerification verification) {
        super(typeFactory, methodName, signatureExtraction, outputExtraction, verification);
        this.procedure = procedure;
    }

    @Override
    protected Set<FunctionTemplate> extractGlobalFunctionTemplates() {
        return TemplateUtils.extractProcedureGlobalFunctionTemplates(typeFactory, procedure);
    }

    @Override
    protected Set<FunctionTemplate> extractLocalFunctionTemplates(Method method) {
        return TemplateUtils.extractProcedureLocalFunctionTemplates(typeFactory, method);
    }

    @Override
    protected List<Method> collectMethods(String methodName) {
        return ExtractionUtils.collectMethods(procedure, methodName);
    }

    @Override
    protected Class<?> getFunctionClass() {
        return procedure;
    }

    @Override
    protected String getHintType() {
        return "Procedure";
    }

    /**
     * Extraction that uses the method return type for producing a {@link FunctionResultTemplate}.
     */
    static ResultExtraction createReturnTypeResultExtraction() {
        return (extractor, method) -> {
            final DataType dataType =
                    DataTypeExtractor.extractFromMethodOutput(
                            extractor.typeFactory,
                            extractor.getFunctionClass(),
                            method,
                            method.getReturnType().getComponentType());
            return FunctionResultTemplate.of(dataType);
        };
    }

    static MethodVerification createParameterAndReturnTypeVerification() {
        return ((method, signature, result) -> {
            // ignore the ProcedureContext in the first argument
            final Class<?>[] parameters =
                    Stream.concat(Stream.of((Class<?>) null), signature.stream())
                            .toArray(Class<?>[]::new);
            final Class<?> returnType = method.getReturnType();
            final boolean isValid =
                    isInvokable(method, parameters)
                            && returnType.isArray()
                            && isAssignable(result, returnType.getComponentType(), true);
            if (!isValid) {
                throw createMethodNotFoundError(
                        method.getName(), parameters, Array.newInstance(result, 0).getClass());
            }
        });
    }
}
