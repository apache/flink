/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OperatorStreamStateHandleTest {

    @Test
    public void testFixedEnumOrder() {

        // Ensure the order / ordinal of all values of enum 'mode' are fixed, as this is used for
        // serialization
        Assertions.assertEquals(0, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE.ordinal());
        Assertions.assertEquals(1, OperatorStateHandle.Mode.UNION.ordinal());
        Assertions.assertEquals(2, OperatorStateHandle.Mode.BROADCAST.ordinal());

        // Ensure all enum values are registered and fixed forever by this test
        Assertions.assertEquals(3, OperatorStateHandle.Mode.values().length);

        // Byte is used to encode enum value on serialization
        Assertions.assertTrue(OperatorStateHandle.Mode.values().length <= Byte.MAX_VALUE);
    }
}
