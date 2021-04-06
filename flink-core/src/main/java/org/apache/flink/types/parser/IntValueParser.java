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

package org.apache.flink.types.parser;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.types.IntValue;

/** Parses a decimal text field into a IntValue. Only characters '1' to '0' and '-' are allowed. */
@PublicEvolving
public class IntValueParser extends FieldParser<IntValue> {

    private static final long OVERFLOW_BOUND = 0x7fffffffL;
    private static final long UNDERFLOW_BOUND = 0x80000000L;

    private IntValue result;

    @Override
    public int parseField(
            byte[] bytes, int startPos, int limit, byte[] delimiter, IntValue reusable) {

        if (startPos == limit) {
            setErrorState(ParseErrorState.EMPTY_COLUMN);
            return -1;
        }

        long val = 0;
        boolean neg = false;

        final int delimLimit = limit - delimiter.length + 1;

        this.result = reusable;

        if (bytes[startPos] == '-') {
            neg = true;
            startPos++;

            // check for empty field with only the sign
            if (startPos == limit
                    || (startPos < delimLimit && delimiterNext(bytes, startPos, delimiter))) {
                setErrorState(ParseErrorState.NUMERIC_VALUE_ORPHAN_SIGN);
                return -1;
            }
        }

        for (int i = startPos; i < limit; i++) {
            if (i < delimLimit && delimiterNext(bytes, i, delimiter)) {
                if (i == startPos) {
                    setErrorState(ParseErrorState.EMPTY_COLUMN);
                    return -1;
                }
                reusable.setValue((int) (neg ? -val : val));
                return i + delimiter.length;
            }
            if (bytes[i] < 48 || bytes[i] > 57) {
                setErrorState(ParseErrorState.NUMERIC_VALUE_ILLEGAL_CHARACTER);
                return -1;
            }
            val *= 10;
            val += bytes[i] - 48;

            if (val > OVERFLOW_BOUND && (!neg || val > UNDERFLOW_BOUND)) {
                setErrorState(ParseErrorState.NUMERIC_VALUE_OVERFLOW_UNDERFLOW);
                return -1;
            }
        }

        reusable.setValue((int) (neg ? -val : val));
        return limit;
    }

    @Override
    public IntValue createValue() {
        return new IntValue();
    }

    @Override
    public IntValue getLastResult() {
        return this.result;
    }
}
