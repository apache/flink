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

package org.apache.flink.core.fs;

import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** Tests for the {@link AutoCloseableRegistry}. */
public class AutoCloseableRegistryTest extends TestLogger {
    @Test
    public void testReverseOrderOfClosing() throws Exception {
        ArrayList<Integer> closeOrder = new ArrayList<>();
        AutoCloseableRegistry autoCloseableRegistry = new AutoCloseableRegistry();
        autoCloseableRegistry.registerCloseable(() -> closeOrder.add(3));
        autoCloseableRegistry.registerCloseable(() -> closeOrder.add(2));
        autoCloseableRegistry.registerCloseable(() -> closeOrder.add(1));

        autoCloseableRegistry.close();

        int expected = 1;
        for (int actual : closeOrder) {
            assertEquals(expected++, actual);
        }
    }

    @Test
    public void testSuppressedExceptions() throws Exception {
        AutoCloseableRegistry autoCloseableRegistry = new AutoCloseableRegistry();
        autoCloseableRegistry.registerCloseable(
                () -> {
                    throw new AssertionError("3");
                });
        autoCloseableRegistry.registerCloseable(
                () -> {
                    throw new Exception("2");
                });
        autoCloseableRegistry.registerCloseable(
                () -> {
                    throw new Exception("1");
                });

        try {
            autoCloseableRegistry.close();

            fail("Close should throw exception");
        } catch (Exception ex) {
            assertEquals("1", ex.getMessage());
            assertEquals("2", ex.getSuppressed()[0].getMessage());
            assertEquals("java.lang.AssertionError: 3", ex.getSuppressed()[1].getMessage());
        }
    }
}
