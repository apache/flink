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

import org.junit.Assert;
import org.junit.Test;

public class KeyGroupRangeTest {

    @Test
    public void testKeyGroupIntersection() {
        KeyGroupRange keyGroupRange1 = KeyGroupRange.of(0, 10);
        KeyGroupRange keyGroupRange2 = KeyGroupRange.of(3, 7);
        KeyGroupRange intersection = keyGroupRange1.getIntersection(keyGroupRange2);
        Assert.assertEquals(3, intersection.getStartKeyGroup());
        Assert.assertEquals(7, intersection.getEndKeyGroup());
        Assert.assertEquals(intersection, keyGroupRange2.getIntersection(keyGroupRange1));

        Assert.assertEquals(keyGroupRange1, keyGroupRange1.getIntersection(keyGroupRange1));

        keyGroupRange1 = KeyGroupRange.of(0, 5);
        keyGroupRange2 = KeyGroupRange.of(6, 10);
        intersection = keyGroupRange1.getIntersection(keyGroupRange2);
        Assert.assertEquals(KeyGroupRange.EMPTY_KEY_GROUP_RANGE, intersection);
        Assert.assertEquals(intersection, keyGroupRange2.getIntersection(keyGroupRange1));

        keyGroupRange1 = KeyGroupRange.of(0, 10);
        keyGroupRange2 = KeyGroupRange.of(5, 20);
        intersection = keyGroupRange1.getIntersection(keyGroupRange2);
        Assert.assertEquals(5, intersection.getStartKeyGroup());
        Assert.assertEquals(10, intersection.getEndKeyGroup());
        Assert.assertEquals(intersection, keyGroupRange2.getIntersection(keyGroupRange1));

        keyGroupRange1 = KeyGroupRange.of(3, 12);
        keyGroupRange2 = KeyGroupRange.of(0, 10);
        intersection = keyGroupRange1.getIntersection(keyGroupRange2);
        Assert.assertEquals(3, intersection.getStartKeyGroup());
        Assert.assertEquals(10, intersection.getEndKeyGroup());
        Assert.assertEquals(intersection, keyGroupRange2.getIntersection(keyGroupRange1));
    }

    @Test
    public void testKeyGroupRangeBasics() {
        testKeyGroupRangeBasicsInternal(0, 0);
        testKeyGroupRangeBasicsInternal(0, 1);
        testKeyGroupRangeBasicsInternal(1, 2);
        testKeyGroupRangeBasicsInternal(42, 42);
        testKeyGroupRangeBasicsInternal(3, 7);
        testKeyGroupRangeBasicsInternal(0, Short.MAX_VALUE);
        testKeyGroupRangeBasicsInternal(Short.MAX_VALUE - 1, Short.MAX_VALUE);

        try {
            testKeyGroupRangeBasicsInternal(-3, 2);
            Assert.fail();
        } catch (IllegalArgumentException ex) {
            // expected
        }
    }

    private void testKeyGroupRangeBasicsInternal(int startKeyGroup, int endKeyGroup) {
        KeyGroupRange keyGroupRange = KeyGroupRange.of(startKeyGroup, endKeyGroup);
        int numberOfKeyGroup = keyGroupRange.getNumberOfKeyGroups();
        Assert.assertEquals(Math.max(0, endKeyGroup - startKeyGroup + 1), numberOfKeyGroup);
        if (keyGroupRange.getNumberOfKeyGroups() > 0) {
            Assert.assertEquals(startKeyGroup, keyGroupRange.getStartKeyGroup());
            Assert.assertEquals(endKeyGroup, keyGroupRange.getEndKeyGroup());
            int c = startKeyGroup;
            for (int i : keyGroupRange) {
                Assert.assertEquals(c, i);
                Assert.assertTrue(keyGroupRange.contains(i));
                ++c;
            }

            Assert.assertEquals(endKeyGroup + 1, c);
            Assert.assertFalse(keyGroupRange.contains(startKeyGroup - 1));
            Assert.assertFalse(keyGroupRange.contains(endKeyGroup + 1));
        } else {
            Assert.assertEquals(KeyGroupRange.EMPTY_KEY_GROUP_RANGE, keyGroupRange);
        }
    }
}
