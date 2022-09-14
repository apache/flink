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

package org.apache.flink.util;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/** Tests for the {@link UnionIterator}. */
public class UnionIteratorTest {

    @Test
    public void testUnion() {
        try {
            UnionIterator<Integer> iter = new UnionIterator<>();

            // should succeed and be empty
            assertFalse(iter.iterator().hasNext());

            iter.clear();

            try {
                iter.iterator().next();
                fail("should fail with an exception");
            } catch (NoSuchElementException e) {
                // expected
            }

            iter.clear();
            iter.addList(Arrays.asList(1, 2, 3, 4, 5, 6, 7));
            iter.addList(Collections.<Integer>emptyList());
            iter.addList(Arrays.asList(8, 9, 10, 11));

            int val = 1;
            for (int i : iter) {
                assertEquals(val++, i);
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testTraversableOnce() {
        try {
            UnionIterator<Integer> iter = new UnionIterator<>();

            // should succeed
            iter.iterator();

            // should fail
            try {
                iter.iterator();
                fail("should fail with an exception");
            } catch (TraversableOnceException e) {
                // expected
            }

            // should fail again
            try {
                iter.iterator();
                fail("should fail with an exception");
            } catch (TraversableOnceException e) {
                // expected
            }

            // reset the thing, keep it empty
            iter.clear();

            // should succeed
            iter.iterator();

            // should fail
            try {
                iter.iterator();
                fail("should fail with an exception");
            } catch (TraversableOnceException e) {
                // expected
            }

            // should fail again
            try {
                iter.iterator();
                fail("should fail with an exception");
            } catch (TraversableOnceException e) {
                // expected
            }

            // reset the thing, add some data
            iter.clear();
            iter.addList(Arrays.asList(1, 2, 3, 4, 5, 6, 7));

            // should succeed
            Iterator<Integer> ints = iter.iterator();
            assertNotNull(ints.next());
            assertNotNull(ints.next());
            assertNotNull(ints.next());

            // should fail if called in the middle of operations
            try {
                iter.iterator();
                fail("should fail with an exception");
            } catch (TraversableOnceException e) {
                // expected
            }

            // reset the thing, keep it empty
            iter.clear();

            // should succeed again
            assertFalse(iter.iterator().hasNext());

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
