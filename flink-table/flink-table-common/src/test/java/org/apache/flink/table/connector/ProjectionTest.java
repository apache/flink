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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProjectionTest {

    @Test
    void testTopLevelProject() {
        assertThat(
                        Projection.of(new int[] {2, 1})
                                .project(
                                        ROW(
                                                FIELD("f0", BIGINT()),
                                                FIELD("f1", STRING()),
                                                FIELD("f2", INT()))))
                .isEqualTo(ROW(FIELD("f2", INT()), FIELD("f1", STRING())));
    }

    @Test
    void testNestedProject() {
        final DataType thirdLevelRow =
                ROW(FIELD("c0", BOOLEAN()), FIELD("c1", DOUBLE()), FIELD("c2", INT()));
        final DataType secondLevelRow =
                ROW(FIELD("b0", BOOLEAN()), FIELD("b1", thirdLevelRow), FIELD("b2", INT()));
        final DataType topLevelRow =
                ROW(FIELD("a0", INT()), FIELD("a1", secondLevelRow), FIELD("a1_b1_c0", INT()));

        assertThat(Projection.of(new int[][] {{0}, {1, 1, 0}}).project(topLevelRow))
                .isEqualTo(ROW(FIELD("a0", INT()), FIELD("a1_b1_c0", BOOLEAN())));
        assertThat(Projection.of(new int[][] {{1, 1}, {0}}).project(topLevelRow))
                .isEqualTo(ROW(FIELD("a1_b1", thirdLevelRow), FIELD("a0", INT())));
        assertThat(
                        Projection.of(new int[][] {{1, 1, 2}, {1, 1, 1}, {1, 1, 0}})
                                .project(topLevelRow))
                .isEqualTo(
                        ROW(
                                FIELD("a1_b1_c2", INT()),
                                FIELD("a1_b1_c1", DOUBLE()),
                                FIELD("a1_b1_c0", BOOLEAN())));
        assertThat(Projection.of(new int[][] {{1, 1, 0}, {2}}).project(topLevelRow))
                .isEqualTo(ROW(FIELD("a1_b1_c0", BOOLEAN()), FIELD("a1_b1_c0_$0", INT())));
    }

    @Test
    void testIsNested() {
        assertThat(Projection.of(new int[] {2, 1}).isNested()).isFalse();
        assertThat(Projection.of(new int[][] {new int[] {1}, new int[] {3}}).isNested()).isFalse();
        assertThat(
                        Projection.of(new int[][] {new int[] {1}, new int[] {1, 2}, new int[] {3}})
                                .isNested())
                .isTrue();
    }

    @Test
    void testDifference() {
        assertThat(
                        Projection.of(new int[] {4, 1, 0, 3, 2})
                                .difference(Projection.of(new int[] {4, 2})))
                .isEqualTo(Projection.of(new int[] {1, 0, 2}));

        assertThat(
                        Projection.of(
                                        new int[][] {
                                            new int[] {4},
                                            new int[] {1, 3},
                                            new int[] {0},
                                            new int[] {3, 1},
                                            new int[] {2}
                                        })
                                .difference(Projection.of(new int[] {4, 2})))
                .isEqualTo(
                        Projection.of(
                                new int[][] {new int[] {1, 3}, new int[] {0}, new int[] {2, 1}}));

        assertThatThrownBy(
                        () ->
                                Projection.of(new int[] {1, 2, 3, 4})
                                        .difference(
                                                Projection.of(
                                                        new int[][] {
                                                            new int[] {2}, new int[] {3, 4}
                                                        })))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testComplement() {
        assertThat(Projection.of(new int[] {4, 1, 2}).complement(5))
                .isEqualTo(Projection.of(new int[] {0, 3}));

        assertThat(
                        Projection.of(new int[][] {new int[] {4}, new int[] {1}, new int[] {2}})
                                .complement(5))
                .isEqualTo(Projection.of(new int[] {0, 3}));

        assertThatThrownBy(
                        () ->
                                Projection.of(
                                                new int[][] {
                                                    new int[] {4}, new int[] {1, 3}, new int[] {2}
                                                })
                                        .complement(10))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testToTopLevelIndexes() {
        assertThat(Projection.of(new int[] {1, 2, 3, 4}).toTopLevelIndexes())
                .isEqualTo(new int[] {1, 2, 3, 4});

        assertThat(
                        Projection.of(new int[][] {new int[] {4}, new int[] {1}, new int[] {2}})
                                .toTopLevelIndexes())
                .isEqualTo(new int[] {4, 1, 2});

        assertThatThrownBy(
                        () ->
                                Projection.of(
                                                new int[][] {
                                                    new int[] {4}, new int[] {1, 3}, new int[] {2}
                                                })
                                        .toTopLevelIndexes())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testToNestedIndexes() {
        assertThat(Projection.of(new int[] {1, 2, 3, 4}).toNestedIndexes())
                .isEqualTo(
                        new int[][] {new int[] {1}, new int[] {2}, new int[] {3}, new int[] {4}});
        assertThat(
                        Projection.of(new int[][] {new int[] {4}, new int[] {1, 3}, new int[] {2}})
                                .toNestedIndexes())
                .isEqualTo(new int[][] {new int[] {4}, new int[] {1, 3}, new int[] {2}});
    }

    @Test
    void testEquals() {
        assertThat(Projection.of(new int[][] {new int[] {1}, new int[] {2}, new int[] {3}}))
                .isEqualTo(Projection.of(new int[] {1, 2, 3}));
    }
}
