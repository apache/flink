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

import org.junit.Assert;
import org.junit.Test;

/** Tests for {@link Tuple2}. */
public class Tuple2Test {

    @Test
    public void testSwapValues() {
        Tuple2<String, Integer> toSwap = new Tuple2<>("Test case", 25);
        Tuple2<Integer, String> swapped = toSwap.swap();

        Assert.assertEquals(swapped.f0, toSwap.f1);

        Assert.assertEquals(swapped.f1, toSwap.f0);
    }

    @Test(expected = NullFieldException.class)
    public void testGetFieldNotNull() {
        Tuple2<String, Integer> tuple = new Tuple2<>("Test case", null);

        Assert.assertEquals("Test case", tuple.getFieldNotNull(0));
        tuple.getFieldNotNull(1);
    }
}
