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

package org.apache.flink.state.api.functions;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Guards against {@link WindowKeyedStateReaderFunction.Context} silently growing a timer accessor
 * (unlike {@link KeyedStateReaderFunction.Context}, it exposes none by design: the operator has
 * never supported timers).
 */
public class WindowKeyedStateReaderFunctionTest {

    @Test
    public void testContextExposesOnlyGetState() {
        Method[] methods = WindowKeyedStateReaderFunction.Context.class.getMethods();
        assertEquals(
                1,
                methods.length,
                "Unexpected method set on WindowKeyedStateReaderFunction.Context: "
                        + Arrays.toString(methods));
        assertEquals("getState", methods[0].getName());
    }
}
