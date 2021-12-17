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

package org.apache.flink.table.planner.functions.casting;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.logical.LogicalType;

/**
 * Cast rule that has code generation capabilities.
 *
 * @param <IN> Input internal type
 * @param <OUT> Output internal type
 */
@Internal
public interface CodeGeneratorCastRule<IN, OUT> extends CastRule<IN, OUT> {

    /**
     * Generates a {@link CastCodeBlock} composed by different statements performing the casting.
     *
     * @see CastCodeBlock
     */
    CastCodeBlock generateCodeBlock(
            Context context,
            String inputTerm,
            String inputIsNullTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType);

    /** Context for code generation. */
    interface Context {
        /** @return where the legacy behaviour should be followed or not. */
        @Deprecated
        boolean legacyBehaviour();

        /** @return the session time zone term */
        String getSessionTimeZoneTerm();

        /**
         * Declare a new variable accessible within the scope of the caller of {@link
         * #generateCodeBlock(Context, String, String, LogicalType, LogicalType)}.
         *
         * @return the variable name
         */
        String declareVariable(String type, String variablePrefix);

        /** @return the term for the type serializer */
        String declareTypeSerializer(LogicalType type);

        /** @return field term. The field is going to be declared as final. */
        String declareClassField(String type, String field, String initialization);
    }
}
