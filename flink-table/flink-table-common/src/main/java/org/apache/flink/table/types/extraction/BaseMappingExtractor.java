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

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.procedures.Procedure;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.extraction.FunctionResultTemplate.FunctionOutputTemplate;
import org.apache.flink.table.types.extraction.FunctionResultTemplate.FunctionStateTemplate;
import org.apache.flink.table.types.extraction.FunctionResultTemplate.FunctionStateTemplate.StateInfoTemplate;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.types.Row;

import org.apache.commons.lang3.ArrayUtils;

import javax.annotation.Nullable;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.EnumSet;
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

    private static final EnumSet<StaticArgumentTrait> SCALAR_TRAITS =
            EnumSet.of(StaticArgumentTrait.SCALAR);

    protected final DataTypeFactory typeFactory;

    protected final String methodName;

    private final SignatureExtraction signatureExtraction;

    protected final ResultExtraction outputExtraction;

    protected final MethodVerification outputVerification;

    public BaseMappingExtractor(
            DataTypeFactory typeFactory,
            String methodName,
            SignatureExtraction signatureExtraction,
            ResultExtraction outputExtraction,
            MethodVerification outputVerification) {
        this.typeFactory = typeFactory;
        this.methodName = methodName;
        this.signatureExtraction = signatureExtraction;
        this.outputExtraction = outputExtraction;
        this.outputVerification = outputVerification;
    }

    Map<FunctionSignatureTemplate, FunctionOutputTemplate> extractOutputMapping() {
        try {
            return extractResultMappings(
                    outputExtraction, FunctionTemplate::getOutputTemplate, outputVerification);
        } catch (Throwable t) {
            throw extractionError(t, "Error in extracting a signature to output mapping.");
        }
    }

    // --------------------------------------------------------------------------------------------
    // Extraction strategies
    // --------------------------------------------------------------------------------------------

    /**
     * Extraction that uses the method parameters for producing a {@link FunctionSignatureTemplate}.
     *
     * @param offset excludes the first n method parameters as arguments. This is necessary for
     *     aggregating functions which don't require a {@link StateHint}. Accumulators are mandatory
     *     and are kind of an implicit {@link StateHint}.
     */
    static SignatureExtraction createArgumentsFromParametersExtraction(
            int offset, @Nullable Class<?> contextClass) {
        return (extractor, method) -> {
            final List<ArgumentParameter> args =
                    extractArgumentParameters(method, offset, contextClass);

            final EnumSet<StaticArgumentTrait>[] argumentTraits = extractArgumentTraits(args);

            final List<FunctionArgumentTemplate> argumentTemplates =
                    extractArgumentTemplates(
                            extractor.typeFactory, extractor.getFunctionClass(), args);

            final String[] argumentNames = extractArgumentNames(method, args);

            final boolean[] argumentOptionals = extractArgumentOptionals(args);

            return FunctionSignatureTemplate.of(
                    argumentTemplates,
                    method.isVarArgs(),
                    argumentTraits,
                    argumentNames,
                    argumentOptionals);
        };
    }

    /**
     * Extraction that uses the method parameters for producing a {@link FunctionSignatureTemplate}.
     *
     * @param offset excludes the first n method parameters as arguments. This is necessary for
     *     aggregating functions which don't require a {@link StateHint}. Accumulators are mandatory
     *     and are kind of an implicit {@link StateHint}.
     */
    static SignatureExtraction createArgumentsFromParametersExtraction(int offset) {
        return createArgumentsFromParametersExtraction(offset, null);
    }

    /** Extraction that uses the method parameters with {@link StateHint} for state entries. */
    static ResultExtraction createStateFromParametersExtraction() {
        return (extractor, method) -> {
            final List<StateParameter> stateParameters = extractStateParameters(method);
            return createStateTemplateFromParameters(extractor, method, stateParameters);
        };
    }

    /**
     * Extraction that uses a generic type variable for producing a {@link FunctionStateTemplate}.
     * Or method parameters with {@link StateHint} for state entries as a fallback.
     */
    static ResultExtraction createStateFromGenericInClassOrParametersExtraction(
            Class<? extends UserDefinedFunction> baseClass, int genericPos) {
        return (extractor, method) -> {
            final List<StateParameter> stateParameters = extractStateParameters(method);
            if (stateParameters.isEmpty()) {
                final DataType dataType =
                        DataTypeExtractor.extractFromGeneric(
                                extractor.typeFactory,
                                baseClass,
                                genericPos,
                                extractor.getFunctionClass());
                final LinkedHashMap<String, StateInfoTemplate> state = new LinkedHashMap<>();
                state.put(
                        UserDefinedFunctionHelper.DEFAULT_ACCUMULATOR_NAME,
                        StateInfoTemplate.of(dataType, null));
                return FunctionResultTemplate.ofState(state);
            }
            return createStateTemplateFromParameters(extractor, method, stateParameters);
        };
    }

    // --------------------------------------------------------------------------------------------
    // Methods for subclasses
    // --------------------------------------------------------------------------------------------

    protected abstract Set<FunctionTemplate> extractGlobalFunctionTemplates();

    protected abstract Set<FunctionTemplate> extractLocalFunctionTemplates(Method method);

    protected abstract List<Method> collectMethods(String methodName);

    protected abstract Class<?> getFunctionClass();

    protected abstract String getHintType();

    protected static Class<?>[] assembleParameters(
            @Nullable FunctionStateTemplate state, FunctionSignatureTemplate arguments) {
        return Stream.concat(
                        Optional.ofNullable(state)
                                .map(FunctionStateTemplate::toClassList)
                                .orElse(List.of())
                                .stream(),
                        arguments.toClassList().stream())
                .toArray(Class[]::new);
    }

    protected static ValidationException createMethodNotFoundError(
            String methodName,
            Class<?>[] parameters,
            @Nullable Class<?> returnType,
            String pattern) {
        return extractionError(
                "Considering all hints, the method should comply with the signature:\n%s%s",
                createMethodSignatureString(methodName, parameters, returnType),
                pattern.isEmpty() ? "" : "\nPattern: " + pattern);
    }

    /**
     * Extracts mappings from signature to result (either state or output) for the entire function.
     * Verifies if the extracted inference matches with the implementation.
     *
     * <p>For example, from {@code (INT, BOOLEAN, ANY) -> INT}. It does this by going through all
     * implementation methods and collecting all "per-method" mappings. The function mapping is the
     * union of all "per-method" mappings.
     */
    @SuppressWarnings("unchecked")
    protected <T extends FunctionResultTemplate>
            Map<FunctionSignatureTemplate, T> extractResultMappings(
                    ResultExtraction resultExtraction,
                    Function<FunctionTemplate, FunctionResultTemplate> accessor,
                    @Nullable MethodVerification verification) {
        final Set<FunctionTemplate> global = extractGlobalFunctionTemplates();
        final Set<FunctionResultTemplate> globalResultOnly =
                findResultOnlyTemplates(global, accessor);

        // for each method find a signature that maps to results
        final Map<FunctionSignatureTemplate, FunctionResultTemplate> collectedMappings =
                new LinkedHashMap<>();
        final List<Method> methods = collectMethods(methodName);
        if (methods.isEmpty()) {
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
        return (Map<FunctionSignatureTemplate, T>) collectedMappings;
    }

    protected static void checkNoState(@Nullable FunctionStateTemplate state) {
        if (state != null) {
            throw extractionError("State is not supported for this kind of function.");
        }
    }

    protected static void checkSingleState(@Nullable FunctionStateTemplate state) {
        if (state == null || state.toClassList().size() != 1) {
            throw extractionError(
                    "Aggregating functions need exactly one state entry for the accumulator.");
        }
    }

    protected static void checkScalarArgumentsOnly(FunctionSignatureTemplate arguments) {
        final EnumSet<StaticArgumentTrait>[] argumentTraits = arguments.argumentTraits;
        IntStream.range(0, argumentTraits.length)
                .forEach(
                        pos -> {
                            if (!argumentTraits[pos].equals(SCALAR_TRAITS)) {
                                throw extractionError(
                                        "Only scalar arguments are supported at this location. "
                                                + "But argument '%s' declared the following traits: %s",
                                        arguments.argumentNames[pos], argumentTraits[pos]);
                            }
                        });
    }

    // --------------------------------------------------------------------------------------------
    // Helper methods
    // --------------------------------------------------------------------------------------------

    private static FunctionStateTemplate createStateTemplateFromParameters(
            BaseMappingExtractor extractor, Method method, List<StateParameter> stateParameters) {
        final String[] argumentNames = extractStateNames(method, stateParameters);
        if (argumentNames == null) {
            throw extractionError("Unable to extract names for all state entries.");
        }

        final List<DataType> dataTypes =
                stateParameters.stream()
                        .map(
                                s ->
                                        DataTypeExtractor.extractFromMethodParameter(
                                                extractor.typeFactory,
                                                extractor.getFunctionClass(),
                                                s.method,
                                                s.pos))
                        .collect(Collectors.toList());

        final LinkedHashMap<String, StateInfoTemplate> state =
                IntStream.range(0, argumentNames.length)
                        .mapToObj(
                                i -> {
                                    final DataType dataType = dataTypes.get(i);
                                    final StateHint hint =
                                            stateParameters
                                                    .get(i)
                                                    .parameter
                                                    .getAnnotation(StateHint.class);
                                    return Map.entry(
                                            argumentNames[i], StateInfoTemplate.of(dataType, hint));
                                })
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        Map.Entry::getValue,
                                        (o, n) -> o,
                                        LinkedHashMap::new));

        return FunctionResultTemplate.ofState(state);
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

    /**
     * Special case for Scala which generates two methods when using var-args (a {@code Seq < String
     * >} and {@code String...}). This method searches for the Java-like variant.
     */
    private static Method correctVarArgMethod(Method method) {
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
            @Nullable MethodVerification verification) {
        if (verification == null) {
            return;
        }
        collectedMappingsPerMethod.forEach(
                (signature, result) -> {
                    if (result instanceof FunctionStateTemplate) {
                        final FunctionStateTemplate stateTemplate = (FunctionStateTemplate) result;
                        verification.verify(method, stateTemplate, signature, null);
                    } else if (result instanceof FunctionOutputTemplate) {
                        final FunctionOutputTemplate outputTemplate =
                                (FunctionOutputTemplate) result;
                        verification.verify(method, null, signature, outputTemplate);
                    }
                });
    }

    // --------------------------------------------------------------------------------------------
    // Parameters extraction (i.e. state and arguments)
    // --------------------------------------------------------------------------------------------

    /** Method parameter that qualifies as a function argument (i.e. not a context or state). */
    private static class ArgumentParameter {
        final Parameter parameter;
        final Method method;
        // Pos in the method, not necessarily in the extracted function
        final int pos;

        private ArgumentParameter(Parameter parameter, Method method, int pos) {
            this.parameter = parameter;
            this.method = method;
            this.pos = pos;
        }
    }

    /** Method parameter that qualifies as a function state (i.e. not a context or argument). */
    private static class StateParameter {
        final Parameter parameter;
        final Method method;
        // Pos in the method, not necessarily in the extracted function
        final int pos;

        private StateParameter(Parameter parameter, Method method, int pos) {
            this.parameter = parameter;
            this.method = method;
            this.pos = pos;
        }
    }

    private static List<ArgumentParameter> extractArgumentParameters(
            Method method, int offset, @Nullable Class<?> contextClass) {
        final Parameter[] parameters = method.getParameters();
        return IntStream.range(0, parameters.length)
                .mapToObj(
                        pos -> {
                            final Parameter parameter = parameters[pos];
                            return new ArgumentParameter(parameter, method, pos);
                        })
                .skip(offset)
                .filter(arg -> contextClass == null || arg.parameter.getType() != contextClass)
                .filter(arg -> arg.parameter.getAnnotation(StateHint.class) == null)
                .collect(Collectors.toList());
    }

    private static List<StateParameter> extractStateParameters(Method method) {
        final Parameter[] parameters = method.getParameters();
        return IntStream.range(0, parameters.length)
                .mapToObj(
                        pos -> {
                            final Parameter parameter = parameters[pos];
                            return new StateParameter(parameter, method, pos);
                        })
                .filter(arg -> arg.parameter.getAnnotation(StateHint.class) != null)
                .collect(Collectors.toList());
    }

    private static List<FunctionArgumentTemplate> extractArgumentTemplates(
            DataTypeFactory typeFactory, Class<?> extractedClass, List<ArgumentParameter> args) {
        return args.stream()
                .map(
                        arg ->
                                // check for input group before start extracting a data type
                                tryExtractInputGroupArgument(arg)
                                        .orElseGet(
                                                () ->
                                                        extractArgumentByKind(
                                                                typeFactory, extractedClass, arg)))
                .collect(Collectors.toList());
    }

    private static Optional<FunctionArgumentTemplate> tryExtractInputGroupArgument(
            ArgumentParameter arg) {
        final DataTypeHint dataTypehint = arg.parameter.getAnnotation(DataTypeHint.class);
        final ArgumentHint argumentHint = arg.parameter.getAnnotation(ArgumentHint.class);
        if (dataTypehint != null && argumentHint != null) {
            throw extractionError(
                    "Argument and data type hints cannot be declared at the same time at position %d.",
                    arg.pos);
        }
        if (argumentHint != null) {
            final DataTypeTemplate template = DataTypeTemplate.fromAnnotation(argumentHint);
            if (template.inputGroup != null) {
                return Optional.of(FunctionArgumentTemplate.ofInputGroup(template.inputGroup));
            }
        } else if (dataTypehint != null) {
            final DataTypeTemplate template = DataTypeTemplate.fromAnnotation(dataTypehint, null);
            if (template.inputGroup != null) {
                return Optional.of(FunctionArgumentTemplate.ofInputGroup(template.inputGroup));
            }
        }
        return Optional.empty();
    }

    private static FunctionArgumentTemplate extractArgumentByKind(
            DataTypeFactory typeFactory, Class<?> extractedClass, ArgumentParameter arg) {
        final Parameter parameter = arg.parameter;
        final ArgumentHint argumentHint = parameter.getAnnotation(ArgumentHint.class);
        final int pos = arg.pos;
        final Set<ArgumentTrait> rootTrait =
                Optional.ofNullable(argumentHint)
                        .map(
                                hint ->
                                        Arrays.stream(hint.value())
                                                .filter(ArgumentTrait::isRoot)
                                                .collect(Collectors.toSet()))
                        .orElse(Set.of(ArgumentTrait.SCALAR));
        if (rootTrait.size() != 1) {
            throw extractionError(
                    "Incorrect argument kind at position %d. Argument kind must be one of: %s",
                    pos,
                    Arrays.stream(ArgumentTrait.values())
                            .filter(ArgumentTrait::isRoot)
                            .collect(Collectors.toList()));
        }

        if (rootTrait.contains(ArgumentTrait.SCALAR)) {
            return extractScalarArgument(typeFactory, extractedClass, arg);
        } else if (rootTrait.contains(ArgumentTrait.ROW_SEMANTIC_TABLE)
                || rootTrait.contains(ArgumentTrait.SET_SEMANTIC_TABLE)) {
            return extractTableArgument(typeFactory, argumentHint, extractedClass, arg);
        } else {
            throw extractionError("Unknown argument kind.");
        }
    }

    private static FunctionArgumentTemplate extractTableArgument(
            DataTypeFactory typeFactory,
            ArgumentHint argumentHint,
            Class<?> extractedClass,
            ArgumentParameter arg) {
        try {
            final DataType type =
                    DataTypeExtractor.extractFromMethodParameter(
                            typeFactory, extractedClass, arg.method, arg.pos);
            return FunctionArgumentTemplate.ofDataType(type);
        } catch (Throwable t) {
            final Class<?> paramClass = arg.parameter.getType();
            final Class<?> argClass = argumentHint.type().bridgedTo();
            if (argClass == Row.class || argClass == RowData.class) {
                return FunctionArgumentTemplate.ofTable(argClass);
            }
            if (paramClass == Row.class || paramClass == RowData.class) {
                return FunctionArgumentTemplate.ofTable(paramClass);
            }
            // Just a regular error for a typed argument
            throw t;
        }
    }

    private static FunctionArgumentTemplate extractScalarArgument(
            DataTypeFactory typeFactory, Class<?> extractedClass, ArgumentParameter arg) {
        final DataType type =
                DataTypeExtractor.extractFromMethodParameter(
                        typeFactory, extractedClass, arg.method, arg.pos);
        // unwrap data type in case of varargs
        if (arg.parameter.isVarArgs()) {
            // for ARRAY
            if (type instanceof CollectionDataType) {
                return FunctionArgumentTemplate.ofDataType(
                        ((CollectionDataType) type).getElementDataType());
            }
            // special case for varargs that have been misinterpreted as BYTES
            else if (type.equals(DataTypes.BYTES())) {
                return FunctionArgumentTemplate.ofDataType(
                        DataTypes.TINYINT().notNull().bridgedTo(byte.class));
            }
        }
        return FunctionArgumentTemplate.ofDataType(type);
    }

    @SuppressWarnings("unchecked")
    private static EnumSet<StaticArgumentTrait>[] extractArgumentTraits(
            List<ArgumentParameter> args) {
        return args.stream()
                .map(
                        arg -> {
                            final ArgumentHint argumentHint =
                                    arg.parameter.getAnnotation(ArgumentHint.class);
                            if (argumentHint == null) {
                                return SCALAR_TRAITS;
                            }
                            final List<StaticArgumentTrait> traits =
                                    Arrays.stream(argumentHint.value())
                                            .map(ArgumentTrait::toStaticTrait)
                                            .collect(Collectors.toList());
                            return EnumSet.copyOf(traits);
                        })
                .toArray(EnumSet[]::new);
    }

    private static @Nullable String[] extractArgumentNames(
            Method method, List<ArgumentParameter> args) {
        final List<String> methodParameterNames =
                ExtractionUtils.extractMethodParameterNames(method);
        if (methodParameterNames != null) {
            return args.stream()
                    .map(arg -> methodParameterNames.get(arg.pos))
                    .toArray(String[]::new);
        } else {
            return null;
        }
    }

    private static @Nullable String[] extractStateNames(Method method, List<StateParameter> state) {
        final List<String> methodParameterNames =
                ExtractionUtils.extractMethodParameterNames(method);
        if (methodParameterNames != null) {
            return state.stream()
                    .map(arg -> methodParameterNames.get(arg.pos))
                    .toArray(String[]::new);
        } else {
            return null;
        }
    }

    private static boolean[] extractArgumentOptionals(List<ArgumentParameter> args) {
        final Boolean[] argumentOptionals =
                args.stream()
                        .map(arg -> arg.parameter.getAnnotation(ArgumentHint.class))
                        .map(
                                hint -> {
                                    if (hint == null) {
                                        return false;
                                    }
                                    return hint.isOptional();
                                })
                        .toArray(Boolean[]::new);
        return ArrayUtils.toPrimitive(argumentOptionals);
    }

    // --------------------------------------------------------------------------------------------
    // Helper interfaces
    // --------------------------------------------------------------------------------------------

    /** Extracts a {@link FunctionSignatureTemplate} from a method. */
    interface SignatureExtraction {
        FunctionSignatureTemplate extract(BaseMappingExtractor extractor, Method method);
    }

    /** Extracts a {@link FunctionResultTemplate} from a class or method. */
    interface ResultExtraction {
        @Nullable
        FunctionResultTemplate extract(BaseMappingExtractor extractor, Method method);
    }

    /** Verifies the signature of a method. */
    interface MethodVerification {
        void verify(
                Method method,
                @Nullable FunctionStateTemplate state,
                FunctionSignatureTemplate arguments,
                @Nullable FunctionOutputTemplate result);
    }
}
