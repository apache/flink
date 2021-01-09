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

package org.apache.flink.api.java.tuple;

import org.apache.flink.types.NullFieldException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link Tuple2}. */
public class Tuple2Test {

    @Test
    public void testSwapValues() {
        Tuple2<String, Integer> toSwap = new Tuple2<>("Test case", 25);
        Tuple2<Integer, String> swapped = toSwap.swap();

        Assertions.assertEquals(swapped.f0, toSwap.f1);

        Assertions.assertEquals(swapped.f1, toSwap.f0);
    }

    @Test
    public void testGetFieldNotNull() {
        assertThrows(
                NullFieldException.class,
                () -> {
                    Tuple2<String, Integer> tuple = new Tuple2<>("Test case", null);

                    Assertions.assertEquals("Test case", tuple.getFieldNotNull(0));
                    tuple.getFieldNotNull(1);
                });
    }
}
