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

package org.apache.flink.table.runtime.functions.scalar;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableConfigValidation;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.utils.FromVariantConverter;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.utils.SimplePath;
import org.apache.flink.types.variant.Variant;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

/**
 * Implementation of {@link BuiltInFunctionDefinitions#VARIANT_GET}.
 *
 * <p>A path that points at nothing gives {@code NULL}: a missing field, an index past the end, a
 * field of a non-object or an element of a non-array. The value found is cast to the result type
 * the way {@code CAST} does, so a variant null becomes {@code NULL} unless the result is a {@code
 * VARIANT}.
 */
@Internal
public class VariantGetFunction extends BuiltInScalarFunction {

    private static final int ARG_PATH = 1;

    private final boolean isTry;

    private final @Nullable SimplePath literalPath;

    // Null when the result is a VARIANT, which needs no cast.
    private final @Nullable FromVariantConverter converter;

    private final String targetTypeName;

    private transient @Nullable String lastPathString;

    private transient @Nullable SimplePath lastPath;

    private transient @Nullable ValidationException lastPathError;

    public VariantGetFunction(SpecializedContext context) {
        this(BuiltInFunctionDefinitions.VARIANT_GET, context, false);
    }

    VariantGetFunction(
            BuiltInFunctionDefinition definition, SpecializedContext context, boolean isTry) {
        super(definition, context);
        this.isTry = isTry;
        this.literalPath = parseLiteralPath(context.getCallContext());
        final LogicalType outputType = getOutputDataType().getLogicalType();
        this.converter =
                outputType.is(LogicalTypeRoot.VARIANT)
                        ? null
                        : FromVariantConverter.create(
                                outputType,
                                TableConfigValidation.getLocalTimeZone(context.getConfiguration()));
        this.targetTypeName = outputType.asSummaryString();
    }

    public @Nullable Object eval(@Nullable Variant variant, @Nullable StringData path) {
        return eval(variant, path, null);
    }

    public @Nullable Object eval(
            @Nullable Variant variant, @Nullable StringData path, @Nullable StringData type) {
        // The type only matters for planning, where it defines the output type.
        if (variant == null || path == null) {
            return null;
        }
        final SimplePath simplePath = literalPath != null ? literalPath : parsePath(path);
        if (simplePath == null) {
            return null;
        }
        final Variant value = navigate(variant, simplePath);
        if (value == null || converter == null) {
            return value;
        }
        if (value.isNull()) {
            return null;
        }
        try {
            return converter.convert(value);
        } catch (RuntimeException e) {
            if (isTry) {
                return null;
            }
            throw new TableRuntimeException(
                    String.format(
                            "Failed to cast the VARIANT value at path '%s' to %s: %s",
                            simplePath, targetTypeName, e.getMessage()),
                    e);
        }
    }

    /**
     * Returns the literal path, or {@code null} to parse the path of each row.
     *
     * <p>The input strategy rejects a malformed literal path. A path that becomes a literal only
     * during optimization, such as a constant column of a view, skips that check, so a malformed
     * one is left to the rules for a path that is not a literal.
     */
    private static @Nullable SimplePath parseLiteralPath(CallContext callContext) {
        if (!callContext.isArgumentLiteral(ARG_PATH)) {
            return null;
        }
        final Optional<String> path = callContext.getArgumentValue(ARG_PATH, String.class);
        if (path.isEmpty()) {
            return null;
        }
        try {
            return SimplePath.parse(path.get());
        } catch (ValidationException e) {
            return null;
        }
    }

    /** Returns the path, or {@code null} for a malformed path in {@code TRY_VARIANT_GET}. */
    private @Nullable SimplePath parsePath(StringData path) {
        final String pathString = path.toString();
        if (!pathString.equals(lastPathString)) {
            lastPathString = pathString;
            try {
                lastPath = SimplePath.parse(pathString);
                lastPathError = null;
            } catch (ValidationException e) {
                lastPath = null;
                lastPathError = e;
            }
        }
        if (lastPathError != null && !isTry) {
            throw new TableRuntimeException(lastPathError.getMessage(), lastPathError);
        }
        return lastPath;
    }

    /** Returns the value at the path, or {@code null} if the path points at nothing. */
    @VisibleForTesting
    static @Nullable Variant navigate(Variant variant, SimplePath path) {
        final List<SimplePath.Step> steps = path.getSteps();
        Variant current = variant;
        for (final SimplePath.Step step : steps) {
            if (step.isField()) {
                current = current.isObject() ? current.getField(step.getFieldName()) : null;
            } else {
                current = current.isArray() ? current.getElement(step.getIndex()) : null;
            }
            if (current == null) {
                return null;
            }
        }
        return current;
    }
}
