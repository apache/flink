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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.ContextResolvedFunction;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.functions.ChangelogFunction;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.extraction.ExtractionUtils;
import org.apache.flink.table.types.inference.StateTypeStrategy;
import org.apache.flink.table.types.inference.TypeInference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.api.internal.TableResultUtils.buildTableResult;
import static org.apache.flink.table.types.inference.TypeInferenceUtil.generateSignature;

/**
 * Operation to describe a FUNCTION.
 *
 * <p>Syntax:
 *
 * <pre>
 * DESCRIBE FUNCTION [EXTENDED] [[catalogName.] dataBasesName].sqlIdentifier
 * </pre>
 */
@Internal
public class DescribeFunctionOperation implements Operation, ExecutableOperation {

    private static final Logger LOG = LoggerFactory.getLogger(DescribeFunctionOperation.class);

    private final UnresolvedIdentifier sqlIdentifier;
    private final boolean isExtended;

    public DescribeFunctionOperation(UnresolvedIdentifier sqlIdentifier, boolean isExtended) {
        this.sqlIdentifier = sqlIdentifier;
        this.isExtended = isExtended;
    }

    public UnresolvedIdentifier getSqlIdentifier() {
        return sqlIdentifier;
    }

    public boolean isExtended() {
        return isExtended;
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("identifier", sqlIdentifier);
        params.put("isExtended", isExtended);
        return OperationUtils.formatWithChildren(
                "DESCRIBE FUNCTION", params, Collections.emptyList(), Operation::asSummaryString);
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        // DESCRIBE FUNCTION <function> shows all the function properties.
        Optional<ContextResolvedFunction> functionOpt =
                ctx.getFunctionCatalog().lookupFunction(sqlIdentifier);
        if (!functionOpt.isPresent()) {
            throw new ValidationException(
                    String.format(
                            "Function with the identifier '%s' doesn't exist.",
                            sqlIdentifier.asSummaryString()));
        }
        final ContextResolvedFunction function = functionOpt.get();
        final CatalogFunction catalogFunction = function.getCatalogFunction();

        List<List<Object>> rows = new ArrayList<>();
        rows.add(Arrays.asList("is system function", String.valueOf(catalogFunction == null)));
        rows.add(Arrays.asList("is temporary", String.valueOf(function.isTemporary())));
        if (catalogFunction != null) {
            rows.add(Arrays.asList("class name", catalogFunction.getClassName()));
            rows.add(
                    Arrays.asList(
                            "function language", catalogFunction.getFunctionLanguage().toString()));
            rows.add(
                    Arrays.asList(
                            "resource uris", catalogFunction.getFunctionResources().toString()));
        }

        if (isExtended) {
            final FunctionDefinition definition = function.getDefinition();
            rows.add(Arrays.asList("kind", definition.getKind().toString()));
            rows.add(Arrays.asList("requirements", definition.getRequirements().toString()));
            rows.add(
                    Arrays.asList(
                            "is deterministic", String.valueOf(definition.isDeterministic())));
            rows.add(
                    Arrays.asList(
                            "supports constant folding",
                            String.valueOf(definition.supportsConstantFolding())));
            final TypeInference typeInference =
                    definition.getTypeInference(ctx.getCatalogManager().getDataTypeFactory());
            rows.add(
                    Arrays.asList(
                            "signature",
                            generateSignature(typeInference, function.toString(), definition)));
            rows.addAll(buildPtfMetadataRows(definition, typeInference));
        }

        return buildTableResult(
                new String[] {"info name", "info value"},
                new DataType[] {DataTypes.STRING(), DataTypes.STRING()},
                rows.stream().map(List::toArray).toArray(Object[][]::new));
    }

    /**
     * Builds supplemental info rows from the given {@link FunctionDefinition} and {@link
     * TypeInference}: a {@code state: <name>} row per state entry (with type and TTL) and, for
     * PROCESS_TABLE functions, three capability rows: {@code accepts system arguments} (whether the
     * framework auto-injects {@code uid} / {@code on_time}), {@code is changelog function} (whether
     * the function implements {@link ChangelogFunction}), and {@code uses timers} (whether the
     * function declares an {@code onTimer} method). Returns an empty list when none apply.
     */
    private static List<List<Object>> buildPtfMetadataRows(
            FunctionDefinition definition, TypeInference typeInference) {
        final List<List<Object>> rows = new ArrayList<>();
        typeInference
                .getStateTypeStrategies()
                .forEach(
                        (name, strategy) ->
                                rows.add(
                                        Arrays.asList(
                                                "state: " + name, formatStateEntry(strategy))));
        if (definition.getKind() == FunctionKind.PROCESS_TABLE) {
            rows.add(
                    Arrays.asList(
                            "accepts system arguments",
                            String.valueOf(!typeInference.disableSystemArguments())));
            rows.add(
                    Arrays.asList(
                            "is changelog function",
                            String.valueOf(definition instanceof ChangelogFunction)));
            rows.add(
                    Arrays.asList(
                            "uses timers",
                            String.valueOf(
                                    !ExtractionUtils.collectMethods(
                                                    definition.getClass(),
                                                    UserDefinedFunctionHelper
                                                            .PROCESS_TABLE_ON_TIMER)
                                            .isEmpty())));
        }
        return rows;
    }

    private static String formatStateEntry(StateTypeStrategy strategy) {
        // We have no CallContext at DESCRIBE time. Many strategies (including TTL lookups in
        // DefaultStateTypeStrategy) ignore the context, but inferType forwards it to the wrapped
        // TypeStrategy which may dereference it. Catch and degrade to <unknown> rather than
        // failing the entire DESCRIBE for one strategy that needs a real CallContext. Log at
        // DEBUG so the cause is diagnosable when a user reports an unexpected <unknown>.
        String typeStr;
        try {
            typeStr = strategy.inferType(null).map(Object::toString).orElse("<unknown>");
        } catch (Exception e) {
            LOG.debug(
                    "Could not infer state type for DESCRIBE FUNCTION; rendering as <unknown>.", e);
            typeStr = "<unknown>";
        }
        final Optional<Duration> ttl;
        try {
            ttl = strategy.getTimeToLive(null);
        } catch (Exception e) {
            LOG.debug(
                    "Could not resolve state TTL for DESCRIBE FUNCTION; rendering as <unknown>.",
                    e);
            return "type=" + typeStr + ", ttl=<unknown>";
        }
        return "type=" + typeStr + ", ttl=" + ttl.map(Duration::toString).orElse("<default>");
    }
}
