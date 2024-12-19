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
import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.extraction.FunctionResultTemplate.FunctionOutputTemplate;
import org.apache.flink.table.types.extraction.FunctionResultTemplate.FunctionStateTemplate;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.extraction.ExtractionUtils.extractionError;

/**
 * Internal representation of a {@link FunctionHint} or {@link ProcedureHint}.
 *
 * <p>All parameters of a template are optional. An empty annotation results in a template where all
 * members are {@code null}.
 */
@Internal
final class FunctionTemplate {

    private final @Nullable FunctionSignatureTemplate signatureTemplate;

    private final @Nullable FunctionStateTemplate stateTemplate;

    private final @Nullable FunctionOutputTemplate outputTemplate;

    private FunctionTemplate(
            @Nullable FunctionSignatureTemplate signatureTemplate,
            @Nullable FunctionStateTemplate stateTemplate,
            @Nullable FunctionOutputTemplate outputTemplate) {
        this.signatureTemplate = signatureTemplate;
        this.stateTemplate = stateTemplate;
        this.outputTemplate = outputTemplate;
    }

    /**
     * Creates an instance using the given {@link FunctionHint}. It resolves explicitly defined data
     * types.
     */
    @SuppressWarnings("deprecation")
    static FunctionTemplate fromAnnotation(DataTypeFactory typeFactory, FunctionHint hint) {
        return new FunctionTemplate(
                createSignatureTemplate(
                        typeFactory,
                        defaultAsNull(hint, FunctionHint::input),
                        defaultAsNull(hint, FunctionHint::argumentNames),
                        defaultAsNull(hint, FunctionHint::argument),
                        defaultAsNull(hint, FunctionHint::arguments),
                        hint.isVarArgs()),
                createStateTemplate(
                        typeFactory,
                        defaultAsNull(hint, FunctionHint::accumulator),
                        defaultAsNull(hint, FunctionHint::state)),
                createOutputTemplate(typeFactory, defaultAsNull(hint, FunctionHint::output)));
    }

    /**
     * Creates an instance using the given {@link ProcedureHint}. It resolves explicitly defined
     * data types.
     */
    @SuppressWarnings("deprecation")
    static FunctionTemplate fromAnnotation(DataTypeFactory typeFactory, ProcedureHint hint) {
        return new FunctionTemplate(
                createSignatureTemplate(
                        typeFactory,
                        defaultAsNull(hint, ProcedureHint::input),
                        defaultAsNull(hint, ProcedureHint::argumentNames),
                        defaultAsNull(hint, ProcedureHint::argument),
                        defaultAsNull(hint, ProcedureHint::arguments),
                        hint.isVarArgs()),
                createStateTemplate(typeFactory, null, null),
                createOutputTemplate(typeFactory, defaultAsNull(hint, ProcedureHint::output)));
    }

    /** Creates an instance of {@link FunctionResultTemplate} from a {@link DataTypeHint}. */
    static @Nullable FunctionOutputTemplate createOutputTemplate(
            DataTypeFactory typeFactory, @Nullable DataTypeHint hint) {
        if (hint == null) {
            return null;
        }
        final DataTypeTemplate template;
        try {
            template = DataTypeTemplate.fromAnnotation(typeFactory, hint);
        } catch (Throwable t) {
            throw extractionError(t, "Error in data type hint annotation.");
        }
        if (template.dataType != null) {
            return FunctionResultTemplate.ofOutput(template.dataType);
        }
        throw extractionError(
                "Data type hint does not specify a data type for use as function result.");
    }

    /** Creates a {@link FunctionStateTemplate}s from {@link StateHint}s or accumulator. */
    static @Nullable FunctionStateTemplate createStateTemplate(
            DataTypeFactory typeFactory,
            @Nullable DataTypeHint accumulatorHint,
            @Nullable StateHint[] stateHints) {
        if (accumulatorHint == null && stateHints == null) {
            return null;
        }
        if (accumulatorHint != null && stateHints != null) {
            throw extractionError(
                    "State hints and accumulator cannot be declared in the same function hint. "
                            + "Use either one or the other.");
        }
        final LinkedHashMap<String, DataType> state = new LinkedHashMap<>();
        if (accumulatorHint != null) {
            state.put("acc", createStateDataType(typeFactory, accumulatorHint, "accumulator"));
            return FunctionResultTemplate.ofState(state);
        }
        IntStream.range(0, stateHints.length)
                .forEach(
                        pos -> {
                            final StateHint hint = stateHints[pos];
                            state.put(
                                    hint.name(),
                                    createStateDataType(typeFactory, hint.type(), "state entry"));
                        });
        return FunctionResultTemplate.ofState(state);
    }

    @Nullable
    FunctionSignatureTemplate getSignatureTemplate() {
        return signatureTemplate;
    }

    @Nullable
    FunctionResultTemplate getStateTemplate() {
        return stateTemplate;
    }

    @Nullable
    FunctionResultTemplate getOutputTemplate() {
        return outputTemplate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FunctionTemplate template = (FunctionTemplate) o;
        return Objects.equals(signatureTemplate, template.signatureTemplate)
                && Objects.equals(stateTemplate, template.stateTemplate)
                && Objects.equals(outputTemplate, template.outputTemplate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(signatureTemplate, stateTemplate, outputTemplate);
    }

    // --------------------------------------------------------------------------------------------

    @ProcedureHint
    @FunctionHint
    @ArgumentHint
    private static class DefaultAnnotationHelper {
        // no implementation
    }

    private static <H extends Annotation> H getDefaultAnnotation(Class<H> hClass) {
        return DefaultAnnotationHelper.class.getAnnotation(hClass);
    }

    private static <T> T defaultAsNull(FunctionHint hint, Function<FunctionHint, T> accessor) {
        return defaultAsNull(hint, getDefaultAnnotation(FunctionHint.class), accessor);
    }

    private static <T> T defaultAsNull(ProcedureHint hint, Function<ProcedureHint, T> accessor) {
        return defaultAsNull(hint, getDefaultAnnotation(ProcedureHint.class), accessor);
    }

    private static <T> T defaultAsNull(ArgumentHint hint, Function<ArgumentHint, T> accessor) {
        return defaultAsNull(hint, getDefaultAnnotation(ArgumentHint.class), accessor);
    }

    private static <T, H extends Annotation> T defaultAsNull(
            H hint, H defaultHint, Function<H, T> accessor) {
        final T defaultValue = accessor.apply(defaultHint);
        final T actualValue = accessor.apply(hint);
        if (Objects.deepEquals(defaultValue, actualValue)) {
            return null;
        }
        return actualValue;
    }

    @SuppressWarnings("unchecked")
    private static @Nullable FunctionSignatureTemplate createSignatureTemplate(
            DataTypeFactory typeFactory,
            @Nullable DataTypeHint[] inputs,
            @Nullable String[] argumentNames,
            @Nullable ArgumentHint[] singularArgumentHints,
            @Nullable ArgumentHint[] pluralArgumentHints,
            boolean isVarArg) {
        // Deal with #argument() and #arguments()
        if (singularArgumentHints != null && pluralArgumentHints != null) {
            throw extractionError(
                    "Argument hints should only be defined once in the same function hint.");
        }
        final ArgumentHint[] argumentHints;
        if (singularArgumentHints != null) {
            argumentHints = singularArgumentHints;
        } else {
            argumentHints = pluralArgumentHints;
        }

        // Deal with #arguments() and #input()
        if (argumentHints != null && inputs != null) {
            throw extractionError(
                    "Argument and input hints cannot be declared in the same function hint. "
                            + "Use either one or the other.");
        }
        final DataTypeHint[] argumentHintTypes;
        final boolean[] argumentOptionals;
        final ArgumentTrait[][] argumentTraits;
        String[] argumentHintNames;
        if (argumentHints != null) {
            argumentHintTypes = new DataTypeHint[argumentHints.length];
            argumentOptionals = new boolean[argumentHints.length];
            argumentTraits = new ArgumentTrait[argumentHints.length][];
            argumentHintNames = new String[argumentHints.length];
            boolean allArgumentNamesNotSet = true;
            for (int i = 0; i < argumentHints.length; i++) {
                final ArgumentHint argumentHint = argumentHints[i];
                argumentHintNames[i] = defaultAsNull(argumentHint, ArgumentHint::name);
                argumentHintTypes[i] = defaultAsNull(argumentHint, ArgumentHint::type);
                argumentOptionals[i] = argumentHint.isOptional();
                argumentTraits[i] = argumentHint.value();
                if (argumentHintNames[i] != null) {
                    allArgumentNamesNotSet = false;
                } else if (!allArgumentNamesNotSet) {
                    throw extractionError(
                            "Argument names in function hint must be either fully set or not set at all.");
                }
            }
            if (allArgumentNamesNotSet) {
                argumentHintNames = null;
            }
        } else if (inputs != null) {
            argumentHintTypes = inputs;
            argumentHintNames = argumentNames;
            argumentOptionals = new boolean[inputs.length];
            argumentTraits = new ArgumentTrait[inputs.length][];
            Arrays.fill(argumentTraits, new ArgumentTrait[] {ArgumentTrait.SCALAR});
        } else {
            return null;
        }

        final List<FunctionArgumentTemplate> argumentTemplates =
                IntStream.range(0, argumentHintTypes.length)
                        .mapToObj(
                                i ->
                                        createArgumentTemplate(
                                                typeFactory,
                                                i,
                                                argumentHintTypes[i],
                                                argumentTraits[i]))
                        .collect(Collectors.toList());

        return FunctionSignatureTemplate.of(
                argumentTemplates,
                isVarArg,
                Arrays.stream(argumentTraits)
                        .map(
                                t -> {
                                    final List<StaticArgumentTrait> traits =
                                            Arrays.stream(t)
                                                    .map(ArgumentTrait::toStaticTrait)
                                                    .collect(Collectors.toList());
                                    return EnumSet.copyOf(traits);
                                })
                        .toArray(EnumSet[]::new),
                argumentHintNames,
                argumentOptionals);
    }

    private static FunctionArgumentTemplate createArgumentTemplate(
            DataTypeFactory typeFactory,
            int pos,
            @Nullable DataTypeHint hint,
            ArgumentTrait[] argumentTraits) {
        final Set<ArgumentTrait> rootTrait =
                Arrays.stream(argumentTraits)
                        .filter(ArgumentTrait::isRoot)
                        .collect(Collectors.toSet());
        if (rootTrait.size() != 1) {
            throw extractionError(
                    "Incorrect argument kind at position %d. Argument kind must be one of: %s",
                    pos,
                    Arrays.stream(ArgumentTrait.values())
                            .filter(ArgumentTrait::isRoot)
                            .collect(Collectors.toList()));
        }

        if (rootTrait.contains(ArgumentTrait.SCALAR)) {
            if (hint != null) {
                final DataTypeTemplate template;
                try {
                    template = DataTypeTemplate.fromAnnotation(typeFactory, hint);
                } catch (Throwable t) {
                    throw extractionError(
                            t,
                            "Error in data type hint annotation for argument at position %s.",
                            pos);
                }
                if (template.dataType != null) {
                    return FunctionArgumentTemplate.ofDataType(template.dataType);
                } else if (template.inputGroup != null) {
                    return FunctionArgumentTemplate.ofInputGroup(template.inputGroup);
                }
            }
            throw extractionError("Data type missing for scalar argument at position %s.", pos);
        } else if (rootTrait.contains(ArgumentTrait.TABLE_AS_ROW)
                || rootTrait.contains(ArgumentTrait.TABLE_AS_SET)) {
            try {
                final DataTypeTemplate template =
                        DataTypeTemplate.fromAnnotation(typeFactory, hint);
                if (template.dataType != null) {
                    return FunctionArgumentTemplate.ofDataType(template.dataType);
                } else if (template.inputGroup != null) {
                    throw extractionError(
                            "Input groups are not supported for table argument at position %s.",
                            pos);
                }
                return FunctionArgumentTemplate.ofTable(Row.class);
            } catch (Throwable t) {
                final Class<?> argClass = hint == null ? Row.class : hint.bridgedTo();
                if (argClass == Row.class || argClass == RowData.class) {
                    return FunctionArgumentTemplate.ofTable(argClass);
                }
                // Just a regular error for a typed argument
                throw t;
            }
        } else {
            throw extractionError("Unknown argument kind.");
        }
    }

    private static DataType createStateDataType(
            DataTypeFactory typeFactory, DataTypeHint dataTypeHint, String description) {
        final DataTypeTemplate template;
        try {
            template = DataTypeTemplate.fromAnnotation(typeFactory, dataTypeHint);
        } catch (Throwable t) {
            throw extractionError(t, "Error in data type hint annotation.");
        }
        if (template.dataType != null) {
            return template.dataType;
        }
        throw extractionError(
                "Data type hint does not specify a data type for use as %s.", description);
    }
}
