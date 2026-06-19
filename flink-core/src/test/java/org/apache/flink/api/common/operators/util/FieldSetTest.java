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

package org.apache.flink.api.common.operators.util;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;

class FieldSetTest {

    @Test
    void testFieldSetConstructors() {
        check(new FieldSet());
        check(FieldSet.EMPTY_SET);
        check(new FieldSet(14), 14);
        check(new FieldSet(Integer.valueOf(3)), 3);
        check(new FieldSet(7, 4, 1), 1, 4, 7);
        check(new FieldSet(7, 4, 1, 4, 7, 1, 4, 2), 1, 4, 2, 7);
    }

    @Test
    void testFieldSetAdds() {
        check(new FieldSet().addField(1).addField(2), 1, 2);
        check(FieldSet.EMPTY_SET.addField(3).addField(2), 3, 2);
        check(new FieldSet(13).addFields(new FieldSet(17, 31, 42)), 17, 13, 42, 31);
        check(new FieldSet(14).addFields(new FieldSet(17)), 17, 14);
        check(new FieldSet(3).addFields(2, 8, 5, 7), 3, 2, 8, 5, 7);
        check(new FieldSet().addFields(new FieldSet()));
        check(new FieldSet().addFields(new FieldSet(3, 4)), 4, 3);
        check(new FieldSet(5, 1).addFields(new FieldSet()), 5, 1);
    }

    @Test
    void testImmutability() {
        FieldSet s1 = new FieldSet();
        FieldSet s2 = new FieldSet(5);
        FieldSet s3 = new FieldSet(Integer.valueOf(7));
        FieldSet s4 = new FieldSet(5, 4, 7, 6);

        s1.addFields(s2).addFields(s3);
        s2.addFields(s4);
        s4.addFields(s1);

        s1.addField(Integer.valueOf(14));
        s2.addFields(78, 13, 66, 3);

        assertThat(s1).isEmpty();
        assertThat(s2).hasSize(1);
        assertThat(s3).hasSize(1);
        assertThat(s4).hasSize(4);
    }

    @Test
    void testAddListToSet() {
        check(new FieldSet().addField(1).addFields(new FieldList(14, 3, 1)), 1, 3, 14);
    }

    private static void check(FieldSet set, int... elements) {
        if (elements == null) {
            assertThat(set).isEmpty();
            return;
        }

        assertThat(set).hasSameSizeAs(elements);

        // test contains
        for (int i : elements) {
            set.contains(i);
        }

        Arrays.sort(elements);

        // test to array
        {
            int[] arr = set.toArray();
            Arrays.sort(arr);
            assertThat(elements).isEqualTo(arr);
        }

        {
            int[] fromIter = new int[set.size()];
            Iterator<Integer> iter = set.iterator();

            for (int i = 0; i < fromIter.length; i++) {
                fromIter[i] = iter.next();
            }
            assertThat(iter).isExhausted();
            Arrays.sort(fromIter);
            assertThat(elements).isEqualTo(fromIter);
        }
    }
}
