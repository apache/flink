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

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.procedures.Procedure;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.table.types.extraction.ExtractionUtils.createMethodSignatureString;
import static org.apache.flink.table.types.extraction.ExtractionUtils.extractionError;
import static org.apache.flink.table.types.extraction.TemplateUtils.findInputOnlyTemplates;
import static org.apache.flink.table.types.extraction.TemplateUtils.findResultMappingTemplates;
import static org.apache.flink.table.types.extraction.TemplateUtils.findResultOnlyTemplate;
import static org.apache.flink.table.types.extraction.TemplateUtils.findResultOnlyTemplates;

/**
 * Base utility for extracting function/procedure mappings from signature to result, e.g. from (INT,
 * STRING) to BOOLEAN.
 *
 * <p>It can not only be used for {@link UserDefinedFunction}, but also for {@link Procedure} which
 * is almost same to {@link UserDefinedFunction} with regard to extracting the mapping from
 * signature to result.
 */
abstract class BaseMappingExtractor {

    protected final DataTypeFactory typeFactory;

    private final String methodName;

    private final SignatureExtraction signatureExtraction;

    protected final ResultExtraction outputExtraction;

    protected final MethodVerification verification;

    public BaseMappingExtractor(
            DataTypeFactory typeFactory,
            String methodName,
            SignatureExtraction signatureExtraction,
            ResultExtraction outputExtraction,
            MethodVerification verification) {
        this.typeFactory = typeFactory;
        this.methodName = methodName;
        this.signatureExtraction = signatureExtraction;
        this.outputExtraction = outputExtraction;
        this.verification = verification;
    }

    protected abstract Set<FunctionTemplate> extractGlobalFunctionTemplates();

    protected abstract Set<FunctionTemplate> extractLocalFunctionTemplates(Method method);

    protected abstract List<Method> collectMethods(String methodName);

    protected abstract Class<?> getFunctionClass();

    protected abstract String getHintType();

    Map<FunctionSignatureTemplate, FunctionResultTemplate> extractOutputMapping() {
        try {
            return extractResultMappings(
                    outputExtraction, FunctionTemplate::getOutputTemplate, verification);
        } catch (Throwable t) {
            throw extractionError(t, "Error in extracting a signature to output mapping.");
        }
    }

    /**
     * Extracts mappings from signature to result (either accumulator or output) for the entire
     * function. Verifies if the extracted inference matches with the implementation.
     *
     * <p>For example, from {@code (INT, BOOLEAN, ANY) -> INT}. It does this by going through all
     * implementation methods and collecting all "per-method" mappings. The function mapping is the
     * union of all "per-method" mappings.
     */
    protected Map<FunctionSignatureTemplate, FunctionResultTemplate> extractResultMappings(
            ResultExtraction resultExtraction,
            Function<FunctionTemplate, FunctionResultTemplate> accessor,
            MethodVerification verification) {
        final Set<FunctionTemplate> global = extractGlobalFunctionTemplates();
        final Set<FunctionResultTemplate> globalResultOnly =
                findResultOnlyTemplates(global, accessor);

        // for each method find a signature that maps to results
        final Map<FunctionSignatureTemplate, FunctionResultTemplate> collectedMappings =
                new LinkedHashMap<>();
        final List<Method> methods = collectMethods(methodName);
        if (methods.size() == 0) {
            throw extractionError(
                    "Could not find a publicly accessible method named '%s'.", methodName);
        }
        for (Method method : methods) {
            try {
                final Method correctMethod = correctVarArgMethod(method);

                final Map<FunctionSignatureTemplate, FunctionResultTemplate>
                        collectedMappingsPerMethod =
                                collectMethodMappings(
                                        correctMethod,
                                        global,
                                        globalResultOnly,
                                        resultExtraction,
                                        accessor);

                // check if the method can be called
                verifyMappingForMethod(correctMethod, collectedMappingsPerMethod, verification);

                // check if method strategies conflict with function strategies
                collectedMappingsPerMethod.forEach(
                        (signature, result) -> putMapping(collectedMappings, signature, result));
            } catch (Throwable t) {
                throw extractionError(
                        t,
                        "Unable to extract a type inference from method:\n%s",
                        method.toString());
            }
        }
        return collectedMappings;
    }

    /**
     * Special case for Scala which generates two methods when using var-args (a {@code Seq < String
     * >} and {@code String...}). This method searches for the Java-like variant.
     */
    static Method correctVarArgMethod(Method method) {
        final int paramCount = method.getParameterCount();
        final Class<?>[] paramClasses = method.getParameterTypes();
        if (paramCount > 0
                && paramClasses[paramCount - 1].getName().equals("scala.collection.Seq")) {
            final Type[] paramTypes = method.getGenericParameterTypes();
            final ParameterizedType seqType = (ParameterizedType) paramTypes[paramCount - 1];
            final Type varArgType = seqType.getActualTypeArguments()[0];
            return ExtractionUtils.collectMethods(method.getDeclaringClass(), method.getName())
                    .stream()
                    .filter(Method::isVarArgs)
                    .filter(candidate -> candidate.getParameterCount() == paramCount)
                    .filter(
                            candidate -> {
                                final Type[] candidateParamTypes =
                                        candidate.getGenericParameterTypes();
                                for (int i = 0; i < paramCount - 1; i++) {
                                    if (candidateParamTypes[i] != paramTypes[i]) {
                                        return false;
                                    }
                                }
                                final Class<?> candidateVarArgType =
                                        candidate.getParameterTypes()[paramCount - 1];
                                return candidateVarArgType.isArray()
                                        &&
                                        // check for Object is needed in case of Scala primitives
                                        // (e.g. Int)
                                        (varArgType == Object.class
                                                || candidateVarArgType.getComponentType()
                                                        == varArgType);
                            })
                    .findAny()
                    .orElse(method);
        }
        return method;
    }

    /**
     * Extracts mappings from signature to result (either accumulator or output) for the given
     * method. It considers both global hints for the entire function and local hints just for this
     * method.
     *
     * <p>The algorithm aims to find an input signature for every declared result. If no result is
     * declared, it will be extracted. If no input signature is declared, it will be extracted.
     */
    private Map<FunctionSignatureTemplate, FunctionResultTemplate> collectMethodMappings(
            Method method,
            Set<FunctionTemplate> global,
            Set<FunctionResultTemplate> globalResultOnly,
            ResultExtraction resultExtraction,
            Function<FunctionTemplate, FunctionResultTemplate> accessor) {
        final Map<FunctionSignatureTemplate, FunctionResultTemplate> collectedMappingsPerMethod =
                new LinkedHashMap<>();
        final Set<FunctionTemplate> local = extractLocalFunctionTemplates(method);

        final Set<FunctionResultTemplate> localResultOnly =
                findResultOnlyTemplates(local, accessor);

        final Set<FunctionTemplate> explicitMappings =
                findResultMappingTemplates(global, local, accessor);

        final FunctionResultTemplate resultOnly =
                findResultOnlyTemplate(
                        globalResultOnly,
                        localResultOnly,
                        explicitMappings,
                        accessor,
                        getHintType());

        final Set<FunctionSignatureTemplate> inputOnly =
                findInputOnlyTemplates(global, local, accessor);

        // add all explicit mappings because they contain complete signatures
        putExplicitMappings(collectedMappingsPerMethod, explicitMappings, inputOnly, accessor);
        // add result only template with explicit or extracted signatures
        putUniqueResultMappings(collectedMappingsPerMethod, resultOnly, inputOnly, method);
        // handle missing result by extraction with explicit or extracted signatures
        putExtractedResultMappings(collectedMappingsPerMethod, inputOnly, resultExtraction, method);

        return collectedMappingsPerMethod;
    }

    // --------------------------------------------------------------------------------------------
    // Helper methods (ordered by invocation order)
    // --------------------------------------------------------------------------------------------
    /** Explicit mappings with complete signature to result declaration. */
    private void putExplicitMappings(
            Map<FunctionSignatureTemplate, FunctionResultTemplate> collectedMappings,
            Set<FunctionTemplate> explicitMappings,
            Set<FunctionSignatureTemplate> signatureOnly,
            Function<FunctionTemplate, FunctionResultTemplate> accessor) {
        explicitMappings.forEach(
                t -> {
                    // signature templates are valid everywhere and are added to the explicit
                    // mapping
                    Stream.concat(signatureOnly.stream(), Stream.of(t.getSignatureTemplate()))
                            .forEach(v -> putMapping(collectedMappings, v, accessor.apply(t)));
                });
    }

    /** Result only template with explicit or extracted signatures. */
    private void putUniqueResultMappings(
            Map<FunctionSignatureTemplate, FunctionResultTemplate> collectedMappings,
            @Nullable FunctionResultTemplate uniqueResult,
            Set<FunctionSignatureTemplate> signatureOnly,
            Method method) {
        if (uniqueResult == null) {
            return;
        }
        // input only templates are valid everywhere if they don't exist fallback to extraction
        if (!signatureOnly.isEmpty()) {
            signatureOnly.forEach(s -> putMapping(collectedMappings, s, uniqueResult));
        } else {
            putMapping(collectedMappings, signatureExtraction.extract(this, method), uniqueResult);
        }
    }

    /** Missing result by extraction with explicit or extracted signatures. */
    private void putExtractedResultMappings(
            Map<FunctionSignatureTemplate, FunctionResultTemplate> collectedMappings,
            Set<FunctionSignatureTemplate> inputOnly,
            ResultExtraction resultExtraction,
            Method method) {
        if (!collectedMappings.isEmpty()) {
            return;
        }
        final FunctionResultTemplate result = resultExtraction.extract(this, method);
        // input only validators are valid everywhere if they don't exist fallback to extraction
        if (!inputOnly.isEmpty()) {
            inputOnly.forEach(signature -> putMapping(collectedMappings, signature, result));
        } else {
            final FunctionSignatureTemplate signature = signatureExtraction.extract(this, method);
            putMapping(collectedMappings, signature, result);
        }
    }

    private void putMapping(
            Map<FunctionSignatureTemplate, FunctionResultTemplate> collectedMappings,
            FunctionSignatureTemplate signature,
            FunctionResultTemplate result) {
        final FunctionResultTemplate existingResult = collectedMappings.get(signature);
        if (existingResult == null) {
            collectedMappings.put(signature, result);
        }
        // template must not conflict with same input
        else if (!existingResult.equals(result)) {
            throw extractionError(
                    String.format(
                            "%s hints with same input definition but different result types are not allowed.",
                            getHintType()));
        }
    }

    /** Checks if the given method can be called and returns what hints declare. */
    private void verifyMappingForMethod(
            Method method,
            Map<FunctionSignatureTemplate, FunctionResultTemplate> collectedMappingsPerMethod,
            MethodVerification verification) {
        collectedMappingsPerMethod.forEach(
                (signature, result) ->
                        verification.verify(method, signature.toClass(), result.toClass()));
    }

    // --------------------------------------------------------------------------------------------
    // Context sensitive extraction and verification logic
    // --------------------------------------------------------------------------------------------

    /**
     * Extraction that uses the method parameters for producing a {@link FunctionSignatureTemplate}.
     */
    static SignatureExtraction createParameterSignatureExtraction(int offset) {
        return (extractor, method) -> {
            final List<FunctionArgumentTemplate> parameterTypes =
                    extractArgumentTemplates(
                            extractor.typeFactory, extractor.getFunctionClass(), method, offset);

            final String[] argumentNames = extractArgumentNames(method, offset);

            return FunctionSignatureTemplate.of(parameterTypes, method.isVarArgs(), argumentNames);
        };
    }

    private static List<FunctionArgumentTemplate> extractArgumentTemplates(
            DataTypeFactory typeFactory, Class<?> extractedClass, Method method, int offset) {
        return IntStream.range(offset, method.getParameterCount())
                .mapToObj(
                        i ->
                                // check for input group before start extracting a data type
                                tryExtractInputGroupArgument(method, i)
                                        .orElseGet(
                                                () ->
                                                        extractDataTypeArgument(
                                                                typeFactory,
                                                                extractedClass,
                                                                method,
                                                                i)))
                .collect(Collectors.toList());
    }

    static Optional<FunctionArgumentTemplate> tryExtractInputGroupArgument(
            Method method, int paramPos) {
        final Parameter parameter = method.getParameters()[paramPos];
        final DataTypeHint hint = parameter.getAnnotation(DataTypeHint.class);
        if (hint != null) {
            final DataTypeTemplate template = DataTypeTemplate.fromAnnotation(hint, null);
            if (template.inputGroup != null) {
                return Optional.of(FunctionArgumentTemplate.of(template.inputGroup));
            }
        }
        return Optional.empty();
    }

    private static FunctionArgumentTemplate extractDataTypeArgument(
            DataTypeFactory typeFactory, Class<?> extractedClass, Method method, int paramPos) {
        final DataType type =
                DataTypeExtractor.extractFromMethodParameter(
                        typeFactory, extractedClass, method, paramPos);
        // unwrap data type in case of varargs
        if (method.isVarArgs() && paramPos == method.getParameterCount() - 1) {
            // for ARRAY
            if (type instanceof CollectionDataType) {
                return FunctionArgumentTemplate.of(
                        ((CollectionDataType) type).getElementDataType());
            }
            // special case for varargs that have been misinterpreted as BYTES
            else if (type.equals(DataTypes.BYTES())) {
                return FunctionArgumentTemplate.of(
                        DataTypes.TINYINT().notNull().bridgedTo(byte.class));
            }
        }
        return FunctionArgumentTemplate.of(type);
    }

    static @Nullable String[] extractArgumentNames(Method method, int offset) {
        final List<String> methodParameterNames =
                ExtractionUtils.extractMethodParameterNames(method);
        if (methodParameterNames != null) {
            return methodParameterNames
                    .subList(offset, methodParameterNames.size())
                    .toArray(new String[0]);
        } else {
            return null;
        }
    }

    protected static ValidationException createMethodNotFoundError(
            String methodName, Class<?>[] parameters, @Nullable Class<?> returnType) {
        return extractionError(
                "Considering all hints, the method should comply with the signature:\n%s",
                createMethodSignatureString(methodName, parameters, returnType));
    }

    // --------------------------------------------------------------------------------------------
    // Helper interfaces
    // --------------------------------------------------------------------------------------------

    /** Extracts a {@link FunctionSignatureTemplate} from a method. */
    protected interface SignatureExtraction {
        FunctionSignatureTemplate extract(BaseMappingExtractor extractor, Method method);
    }

    /** Extracts a {@link FunctionResultTemplate} from a class or method. */
    protected interface ResultExtraction {
        @Nullable
        FunctionResultTemplate extract(BaseMappingExtractor extractor, Method method);
    }

    /** Verifies the signature of a method. */
    protected interface MethodVerification {
        void verify(Method method, List<Class<?>> arguments, Class<?> result);
    }
}
