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
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.catalog.DataTypeFactory;

import javax.annotation.Nullable;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

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

    private final @Nullable FunctionResultTemplate accumulatorTemplate;

    private final @Nullable FunctionResultTemplate outputTemplate;

    private FunctionTemplate(
            @Nullable FunctionSignatureTemplate signatureTemplate,
            @Nullable FunctionResultTemplate accumulatorTemplate,
            @Nullable FunctionResultTemplate outputTemplate) {
        this.signatureTemplate = signatureTemplate;
        this.accumulatorTemplate = accumulatorTemplate;
        this.outputTemplate = outputTemplate;
    }

    /**
     * Creates an instance using the given {@link FunctionHint}. It resolves explicitly defined data
     * types.
     */
    static FunctionTemplate fromAnnotation(DataTypeFactory typeFactory, FunctionHint hint) {
        return new FunctionTemplate(
                createSignatureTemplate(
                        typeFactory,
                        defaultAsNull(hint, FunctionHint::input),
                        defaultAsNull(hint, FunctionHint::argumentNames),
                        hint.isVarArgs()),
                createResultTemplate(typeFactory, defaultAsNull(hint, FunctionHint::accumulator)),
                createResultTemplate(typeFactory, defaultAsNull(hint, FunctionHint::output)));
    }

    /**
     * Creates an instance using the given {@link ProcedureHint}. It resolves explicitly defined
     * data types.
     */
    static FunctionTemplate fromAnnotation(DataTypeFactory typeFactory, ProcedureHint hint) {
        return new FunctionTemplate(
                createSignatureTemplate(
                        typeFactory,
                        defaultAsNull(hint, ProcedureHint::input),
                        defaultAsNull(hint, ProcedureHint::argumentNames),
                        hint.isVarArgs()),
                null,
                createResultTemplate(typeFactory, defaultAsNull(hint, ProcedureHint::output)));
    }

    /** Creates an instance of {@link FunctionResultTemplate} from a {@link DataTypeHint}. */
    static @Nullable FunctionResultTemplate createResultTemplate(
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
            return FunctionResultTemplate.of(template.dataType);
        }
        throw extractionError(
                "Data type hint does not specify a data type for use as function result.");
    }

    @Nullable
    FunctionSignatureTemplate getSignatureTemplate() {
        return signatureTemplate;
    }

    @Nullable
    FunctionResultTemplate getAccumulatorTemplate() {
        return accumulatorTemplate;
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
                && Objects.equals(accumulatorTemplate, template.accumulatorTemplate)
                && Objects.equals(outputTemplate, template.outputTemplate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(signatureTemplate, accumulatorTemplate, outputTemplate);
    }

    // --------------------------------------------------------------------------------------------

    @ProcedureHint
    @FunctionHint
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

    private static <T, H extends Annotation> T defaultAsNull(
            H hint, H defaultHint, Function<H, T> accessor) {
        final T defaultValue = accessor.apply(defaultHint);
        final T actualValue = accessor.apply(hint);
        if (Objects.deepEquals(defaultValue, actualValue)) {
            return null;
        }
        return actualValue;
    }

    private static @Nullable FunctionSignatureTemplate createSignatureTemplate(
            DataTypeFactory typeFactory,
            @Nullable DataTypeHint[] input,
            @Nullable String[] argumentNames,
            boolean isVarArg) {
        if (input == null) {
            return null;
        }
        return FunctionSignatureTemplate.of(
                Arrays.stream(input)
                        .map(dataTypeHint -> createArgumentTemplate(typeFactory, dataTypeHint))
                        .collect(Collectors.toList()),
                isVarArg,
                argumentNames);
    }

    private static FunctionArgumentTemplate createArgumentTemplate(
            DataTypeFactory typeFactory, DataTypeHint hint) {
        final DataTypeTemplate template = DataTypeTemplate.fromAnnotation(typeFactory, hint);
        if (template.dataType != null) {
            return FunctionArgumentTemplate.of(template.dataType);
        } else if (template.inputGroup != null) {
            return FunctionArgumentTemplate.of(template.inputGroup);
        }
        throw extractionError(
                "Data type hint does neither specify a data type nor input group for use as function argument.");
    }
}
