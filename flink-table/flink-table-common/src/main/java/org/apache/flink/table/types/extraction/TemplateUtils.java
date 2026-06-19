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
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.procedures.Procedure;

import javax.annotation.Nullable;

import java.lang.reflect.Method;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.types.extraction.ExtractionUtils.collectAnnotationsOfClass;
import static org.apache.flink.table.types.extraction.ExtractionUtils.collectAnnotationsOfMethod;
import static org.apache.flink.table.types.extraction.ExtractionUtils.extractionError;

/** Utilities for extracting and dealing with templates. */
@Internal
final class TemplateUtils {

    /** Retrieve global templates from function class. */
    static Set<FunctionTemplate> extractGlobalFunctionTemplates(
            DataTypeFactory typeFactory, Class<? extends UserDefinedFunction> function) {
        return asFunctionTemplates(
                typeFactory, collectAnnotationsOfClass(FunctionHint.class, function));
    }

    /** Retrieve global templates from procedure class. */
    static Set<FunctionTemplate> extractProcedureGlobalFunctionTemplates(
            DataTypeFactory typeFactory, Class<? extends Procedure> procedure) {
        return asFunctionTemplatesForProcedure(
                typeFactory, collectAnnotationsOfClass(ProcedureHint.class, procedure));
    }

    /** Retrieve local templates from function method. */
    static Set<FunctionTemplate> extractLocalFunctionTemplates(
            DataTypeFactory typeFactory, Method method) {
        return asFunctionTemplates(
                typeFactory, collectAnnotationsOfMethod(FunctionHint.class, method));
    }

    /** Retrieve local templates from procedure method. */
    static Set<FunctionTemplate> extractProcedureLocalFunctionTemplates(
            DataTypeFactory typeFactory, Method method) {
        return asFunctionTemplatesForProcedure(
                typeFactory, collectAnnotationsOfMethod(ProcedureHint.class, method));
    }

    /** Converts {@link FunctionHint}s to {@link FunctionTemplate}. */
    static Set<FunctionTemplate> asFunctionTemplates(
            DataTypeFactory typeFactory, Set<FunctionHint> hints) {
        return hints.stream()
                .map(
                        hint -> {
                            try {
                                return FunctionTemplate.fromAnnotation(typeFactory, hint);
                            } catch (Throwable t) {
                                throw extractionError(t, "Error in function hint annotation.");
                            }
                        })
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /** Converts {@link ProcedureHint}s to {@link FunctionTemplate}. */
    static Set<FunctionTemplate> asFunctionTemplatesForProcedure(
            DataTypeFactory typeFactory, Set<ProcedureHint> hints) {
        return hints.stream()
                .map(
                        hint -> {
                            try {
                                return FunctionTemplate.fromAnnotation(typeFactory, hint);
                            } catch (Throwable t) {
                                throw extractionError(t, "Error in procedure hint annotation.");
                            }
                        })
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /** Find a template that only specifies a result. */
    static Set<FunctionResultTemplate> findResultOnlyTemplates(
            Set<FunctionTemplate> functionTemplates,
            Function<FunctionTemplate, FunctionResultTemplate> accessor) {
        return functionTemplates.stream()
                .filter(t -> t.getSignatureTemplate() == null && accessor.apply(t) != null)
                .map(accessor)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /** Hints that only declare a result (either accumulator or output). */
    static @Nullable FunctionResultTemplate findResultOnlyTemplate(
            Set<FunctionResultTemplate> globalResultOnly,
            Set<FunctionResultTemplate> localResultOnly,
            Set<FunctionTemplate> explicitMappings,
            Function<FunctionTemplate, FunctionResultTemplate> accessor,
            String hintType) {
        final Set<FunctionResultTemplate> resultOnly =
                Stream.concat(globalResultOnly.stream(), localResultOnly.stream())
                        .collect(Collectors.toCollection(LinkedHashSet::new));
        final Set<FunctionResultTemplate> allResults =
                Stream.concat(resultOnly.stream(), explicitMappings.stream().map(accessor))
                        .collect(Collectors.toCollection(LinkedHashSet::new));
        if (resultOnly.size() == 1 && allResults.size() == 1) {
            return resultOnly.stream().findFirst().orElse(null);
        }
        // different results is only fine as long as those come from a mapping
        if (resultOnly.size() > 1 || (!resultOnly.isEmpty() && !explicitMappings.isEmpty())) {
            throw extractionError(
                    String.format(
                            "%s hints that lead to ambiguous results are not allowed.", hintType));
        }
        return null;
    }

    /** Hints that map a signature to a result. */
    static Set<FunctionTemplate> findResultMappingTemplates(
            Set<FunctionTemplate> globalTemplates,
            Set<FunctionTemplate> localTemplates,
            Function<FunctionTemplate, FunctionResultTemplate> accessor) {
        return Stream.concat(globalTemplates.stream(), localTemplates.stream())
                .filter(t -> t.getSignatureTemplate() != null && accessor.apply(t) != null)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /** Hints that only declare an input. */
    static Set<FunctionSignatureTemplate> findInputOnlyTemplates(
            Set<FunctionTemplate> global,
            Set<FunctionTemplate> local,
            Function<FunctionTemplate, FunctionResultTemplate> accessor) {
        return Stream.concat(global.stream(), local.stream())
                .filter(t -> t.getSignatureTemplate() != null && accessor.apply(t) == null)
                .map(FunctionTemplate::getSignatureTemplate)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private TemplateUtils() {
        // no instantiation
    }
}
