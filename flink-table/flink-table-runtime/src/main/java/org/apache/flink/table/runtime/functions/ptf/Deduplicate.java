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

package org.apache.flink.table.runtime.functions.ptf;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.runtime.functions.BuiltInSpecializedFunction;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.strategies.DeduplicateTypeStrategy;
import org.apache.flink.table.types.inference.strategies.DeduplicateTypeStrategyUtils.Keep;

/**
 * Runtime implementation of {@link BuiltInFunctionDefinitions#DEDUPLICATE}.
 *
 * <pre>
 * keep    sequence_by  implementation
 * first   absent       {@link DeduplicateEmitOnceFunction}
 * first   present      {@link DeduplicateCurrentRowFunction}
 * last    any          {@link DeduplicateCurrentRowFunction}
 * </pre>
 */
@Internal
public class Deduplicate extends BuiltInSpecializedFunction {

    public Deduplicate() {
        super(BuiltInFunctionDefinitions.DEDUPLICATE);
    }

    @Override
    public UserDefinedFunction specialize(final SpecializedContext context) {
        final CallContext callContext = context.getCallContext();
        final boolean isKeepFirst = DeduplicateTypeStrategy.keep(callContext) == Keep.FIRST;

        if (isKeepFirst) {
            final boolean hasSequenceBy =
                    DeduplicateTypeStrategy.sequenceByOutputIndices(callContext).length > 0;
            if (!hasSequenceBy) {
                return new DeduplicateEmitOnceFunction(context);
            } else {
                return new DeduplicateCurrentRowFunction(context);
            }
        } else {
            return new DeduplicateCurrentRowFunction(context);
        }
    }
}
