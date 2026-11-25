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
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.types.variant.BinaryVariant;
import org.apache.flink.types.variant.Variant;
import org.apache.flink.types.variant.VariantValidator;

import javax.annotation.Nullable;

/** Implementation of {@link BuiltInFunctionDefinitions#IS_VALID_VARIANT}. */
@Internal
public final class IsValidVariantFunction extends BuiltInScalarFunction {

    private transient VariantValidator validator;

    public IsValidVariantFunction(SpecializedContext context) {
        super(BuiltInFunctionDefinitions.IS_VALID_VARIANT, context);
    }

    @Override
    public void open(final FunctionContext context) {
        validator = new VariantValidator();
    }

    public @Nullable Boolean eval(final @Nullable Variant variant) {
        if (variant == null) {
            return null;
        }
        return validator.isValid((BinaryVariant) variant);
    }
}
