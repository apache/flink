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

package org.apache.flink.table.utils.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.functions.TableFunction;

import java.util.Arrays;

/** Emits one-based ordinals for equally sized arrays, treating null lengths as zero. */
@Internal
public class ExplodeOrdinalityFunction extends TableFunction<Integer> {

    private static final long serialVersionUID = 1L;

    public void eval(Integer... lengths) {
        if (lengths.length == 0) {
            return;
        }
        final int length = lengths[0] == null ? 0 : lengths[0];
        for (final Integer otherLength : lengths) {
            if ((otherLength == null ? 0 : otherLength) != length) {
                throw new IllegalArgumentException(
                        "Arrays to explode must have the same length: " + Arrays.toString(lengths));
            }
        }
        for (int i = 0; i < length; i++) {
            collect(i + 1);
        }
    }
}
