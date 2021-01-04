/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.slots;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/** Tests for the {@link ResourceCounter}. */
public class ResourceCounterTest extends TestLogger {

    @Test
    public void testIncrement() {
        ResourceCounter counter = new ResourceCounter();

        counter.incrementCount(ResourceProfile.ANY, 1);
        assertThat(counter.getResourceCount(ResourceProfile.ANY), is(1));
    }

    @Test
    public void testDecrement() {
        ResourceCounter counter = new ResourceCounter();

        counter.incrementCount(ResourceProfile.ANY, 1);
        counter.decrementCount(ResourceProfile.ANY, 1);
        assertThat(counter.getResourceCount(ResourceProfile.ANY), is(0));

        assertThat(counter.isEmpty(), is(true));
    }

    @Test
    public void testCopy() {
        ResourceCounter counter = new ResourceCounter();
        counter.incrementCount(ResourceProfile.ANY, 1);

        ResourceCounter copy = counter.copy();
        counter.decrementCount(ResourceProfile.ANY, 1);

        // check that copy is independent from original
        assertThat(copy.getResourceCount(ResourceProfile.ANY), is(1));
    }
}
