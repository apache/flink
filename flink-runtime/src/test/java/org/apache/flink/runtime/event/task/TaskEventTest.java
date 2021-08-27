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

package org.apache.flink.runtime.event.task;

import org.apache.flink.util.InstantiationUtil;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This class contains serialization tests concerning task events derived from {@link
 * org.apache.flink.runtime.event.AbstractEvent}.
 */
public class TaskEventTest {
    /** This test checks the serialization/deserialization of {@link IntegerTaskEvent} objects. */
    @Test
    public void testIntegerTaskEvent() {

        try {
            final IntegerTaskEvent orig = new IntegerTaskEvent(11);
            final IntegerTaskEvent copy = InstantiationUtil.createCopyWritable(orig);

            assertEquals(orig.getInteger(), copy.getInteger());
            assertEquals(orig.hashCode(), copy.hashCode());
            assertTrue(orig.equals(copy));

        } catch (IOException ioe) {
            fail(ioe.getMessage());
        }
    }

    /** This test checks the serialization/deserialization of {@link StringTaskEvent} objects. */
    @Test
    public void testStringTaskEvent() {

        try {

            final StringTaskEvent orig = new StringTaskEvent("Test");
            final StringTaskEvent copy = InstantiationUtil.createCopyWritable(orig);

            assertEquals(orig.getString(), copy.getString());
            assertEquals(orig.hashCode(), copy.hashCode());
            assertTrue(orig.equals(copy));

        } catch (IOException ioe) {
            fail(ioe.getMessage());
        }
    }
}
