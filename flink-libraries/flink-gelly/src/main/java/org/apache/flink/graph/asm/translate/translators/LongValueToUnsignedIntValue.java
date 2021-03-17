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

package org.apache.flink.graph.asm.translate.translators;

import org.apache.flink.graph.asm.translate.TranslateFunction;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;

/**
 * Translate {@link LongValue} to {@link IntValue}.
 *
 * <p>Throws {@link RuntimeException} for integer overflow.
 */
public class LongValueToUnsignedIntValue implements TranslateFunction<LongValue, IntValue> {

    public static final long MAX_VERTEX_COUNT = 1L << 32;

    @Override
    public IntValue translate(LongValue value, IntValue reuse) throws Exception {
        if (reuse == null) {
            reuse = new IntValue();
        }

        long l = value.getValue();

        if (l < 0 || l >= MAX_VERTEX_COUNT) {
            throw new IllegalArgumentException("Cannot cast long value " + value + " to integer.");
        }

        reuse.setValue((int) (l & (MAX_VERTEX_COUNT - 1)));
        return reuse;
    }
}
