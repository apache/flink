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

package org.apache.flink.table.planner.typeutils;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/** Tests for {@link RowTypeUtils}. */
public class RowTypeUtilsTest {

    @Test
    public void testGetUniqueName() {
        Assert.assertEquals(
                Arrays.asList("Dave", "Evan"),
                RowTypeUtils.getUniqueName(
                        Arrays.asList("Dave", "Evan"), Arrays.asList("Alice", "Bob")));
        Assert.assertEquals(
                Arrays.asList("Bob_0", "Bob_1", "Dave", "Alice_0"),
                RowTypeUtils.getUniqueName(
                        Arrays.asList("Bob", "Bob", "Dave", "Alice"),
                        Arrays.asList("Alice", "Bob")));
    }
}
