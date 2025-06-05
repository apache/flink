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

package org.apache.flink.table.runtime.operators.join.lookup.utils;

import java.util.Queue;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Util functions for lookup join test. */
public class AsyncLookupTestUtils {

    public static void assertKeyOrdered(Queue<?> actual, Queue<?> expected) {
        assertFalse(actual.isEmpty());
        assertFalse(expected.isEmpty());
        for (Object element : actual) {
            if (element.equals(expected.peek())) {
                expected.poll();
            }
        }
        assertTrue(expected.isEmpty());
    }
}
