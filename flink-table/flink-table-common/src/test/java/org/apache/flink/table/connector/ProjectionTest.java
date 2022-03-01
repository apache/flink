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

package org.apache.flink.table.connector;

import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.Test;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProjectionTest {

    @Test
    void testTopLevelProject() {
        assertEquals(
                ROW(FIELD("f2", INT()), FIELD("f1", STRING())),
                Projection.of(new int[] {2, 1})
                        .project(
                                ROW(
                                        FIELD("f0", BIGINT()),
                                        FIELD("f1", STRING()),
                                        FIELD("f2", INT()))));
    }

    @Test
    void testNestedProject() {
        final DataType thirdLevelRow =
                ROW(FIELD("c0", BOOLEAN()), FIELD("c1", DOUBLE()), FIELD("c2", INT()));
        final DataType secondLevelRow =
                ROW(FIELD("b0", BOOLEAN()), FIELD("b1", thirdLevelRow), FIELD("b2", INT()));
        final DataType topLevelRow =
                ROW(FIELD("a0", INT()), FIELD("a1", secondLevelRow), FIELD("a1_b1_c0", INT()));

        assertEquals(
                ROW(FIELD("a0", INT()), FIELD("a1_b1_c0", BOOLEAN())),
                Projection.of(new int[][] {{0}, {1, 1, 0}}).project(topLevelRow));
        assertEquals(
                ROW(FIELD("a1_b1", thirdLevelRow), FIELD("a0", INT())),
                Projection.of(new int[][] {{1, 1}, {0}}).project(topLevelRow));
        assertEquals(
                ROW(
                        FIELD("a1_b1_c2", INT()),
                        FIELD("a1_b1_c1", DOUBLE()),
                        FIELD("a1_b1_c0", BOOLEAN())),
                Projection.of(new int[][] {{1, 1, 2}, {1, 1, 1}, {1, 1, 0}}).project(topLevelRow));
        assertEquals(
                ROW(FIELD("a1_b1_c0", BOOLEAN()), FIELD("a1_b1_c0_$0", INT())),
                Projection.of(new int[][] {{1, 1, 0}, {2}}).project(topLevelRow));
    }

    @Test
    void testIsNested() {
        assertFalse(Projection.of(new int[] {2, 1}).isNested());
        assertFalse(Projection.of(new int[][] {new int[] {1}, new int[] {3}}).isNested());
        assertTrue(
                Projection.of(new int[][] {new int[] {1}, new int[] {1, 2}, new int[] {3}})
                        .isNested());
    }

    @Test
    void testDifference() {
        assertEquals(
                Projection.of(new int[] {1, 0, 2}),
                Projection.of(new int[] {4, 1, 0, 3, 2})
                        .difference(Projection.of(new int[] {4, 2})));

        assertEquals(
                Projection.of(new int[][] {new int[] {1, 3}, new int[] {0}, new int[] {2, 1}}),
                Projection.of(
                                new int[][] {
                                    new int[] {4},
                                    new int[] {1, 3},
                                    new int[] {0},
                                    new int[] {3, 1},
                                    new int[] {2}
                                })
                        .difference(Projection.of(new int[] {4, 2})));

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        Projection.of(new int[] {1, 2, 3, 4})
                                .difference(
                                        Projection.of(
                                                new int[][] {new int[] {2}, new int[] {3, 4}})));
    }

    @Test
    void testComplement() {
        assertEquals(
                Projection.of(new int[] {0, 3}), Projection.of(new int[] {4, 1, 2}).complement(5));

        assertEquals(
                Projection.of(new int[] {0, 3}),
                Projection.of(new int[][] {new int[] {4}, new int[] {1}, new int[] {2}})
                        .complement(5));

        assertThrows(
                IllegalStateException.class,
                () ->
                        Projection.of(new int[][] {new int[] {4}, new int[] {1, 3}, new int[] {2}})
                                .complement(10));
    }

    @Test
    void testToTopLevelIndexes() {
        assertArrayEquals(
                new int[] {1, 2, 3, 4}, Projection.of(new int[] {1, 2, 3, 4}).toTopLevelIndexes());

        assertArrayEquals(
                new int[] {4, 1, 2},
                Projection.of(new int[][] {new int[] {4}, new int[] {1}, new int[] {2}})
                        .toTopLevelIndexes());

        assertThrows(
                IllegalStateException.class,
                () ->
                        Projection.of(new int[][] {new int[] {4}, new int[] {1, 3}, new int[] {2}})
                                .toTopLevelIndexes());
    }

    @Test
    void testToNestedIndexes() {
        assertArrayEquals(
                new int[][] {new int[] {1}, new int[] {2}, new int[] {3}, new int[] {4}},
                Projection.of(new int[] {1, 2, 3, 4}).toNestedIndexes());
        assertArrayEquals(
                new int[][] {new int[] {4}, new int[] {1, 3}, new int[] {2}},
                Projection.of(new int[][] {new int[] {4}, new int[] {1, 3}, new int[] {2}})
                        .toNestedIndexes());
    }

    @Test
    void testEquals() {
        assertEquals(
                Projection.of(new int[] {1, 2, 3}),
                Projection.of(new int[][] {new int[] {1}, new int[] {2}, new int[] {3}}));
    }
}
