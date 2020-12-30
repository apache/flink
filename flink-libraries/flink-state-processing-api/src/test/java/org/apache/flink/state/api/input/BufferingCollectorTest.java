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

package org.apache.flink.state.api.input;

import org.junit.Assert;
import org.junit.Test;

/** Test of the buffering collector. */
public class BufferingCollectorTest {

    @Test
    public void testNestRemovesElement() {
        BufferingCollector<Integer> collector = new BufferingCollector<>();

        collector.collect(1);

        Assert.assertTrue("Failed to add element to collector", collector.hasNext());
        Assert.assertEquals(
                "Incorrect element removed from collector", Integer.valueOf(1), collector.next());
        Assert.assertFalse("Failed to drop element from collector", collector.hasNext());
    }

    @Test
    public void testEmptyCollectorReturnsNull() {
        BufferingCollector<Integer> collector = new BufferingCollector<>();
        Assert.assertNull("Empty collector did not return null", collector.next());
    }
}
