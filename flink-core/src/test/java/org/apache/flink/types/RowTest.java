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
package org.apache.flink.types;

import org.apache.flink.api.java.tuple.Tuple2;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

/** Tests for {@link Row} and {@link RowUtils}. */
public class RowTest {

    @Test
    public void testRowNamed() {
        final Row row = Row.withNames(RowKind.DELETE);

        // test getters and setters
        row.setField("a", 42);
        row.setField("b", true);
        row.setField("c", null);
        assertThat(row.getFieldNames(false), containsInAnyOrder("a", "b", "c"));
        assertThat(row.getArity(), equalTo(3));
        assertThat(row.getKind(), equalTo(RowKind.DELETE));
        assertThat(row.getField("a"), equalTo(42));
        assertThat(row.getField("b"), equalTo(true));
        assertThat(row.getField("c"), equalTo(null));

        // test toString
        assertThat(row.toString(), equalTo("-D{a=42, b=true, c=null}"));

        // test override
        row.setField("a", 13);
        row.setField("c", "Hello");
        assertThat(row.getField("a"), equalTo(13));
        assertThat(row.getField("b"), equalTo(true));
        assertThat(row.getField("c"), equalTo("Hello"));

        // test equality
        final Row otherRow1 = Row.withNames(RowKind.DELETE);
        otherRow1.setField("a", 13);
        otherRow1.setField("b", true);
        otherRow1.setField("c", "Hello");
        assertThat(row.hashCode(), equalTo(otherRow1.hashCode()));
        assertThat(row, equalTo(otherRow1));

        // test inequality
        final Row otherRow2 = Row.withNames(RowKind.DELETE);
        otherRow2.setField("a", 13);
        otherRow2.setField("b", false); // diff here
        otherRow2.setField("c", "Hello");
        assertThat(row.hashCode(), not(equalTo(otherRow2.hashCode())));
        assertThat(row, not(equalTo(otherRow2)));

        // test clear
        row.clear();
        assertThat(row.getArity(), equalTo(0));
        assertThat(row.getFieldNames(false), empty());
        assertThat(row.toString(), equalTo("-D{}"));

        // test invalid setter
        try {
            row.setField(0, 13);
            fail();
        } catch (Throwable t) {
            assertThat(t, hasMessage(containsString("not supported in name-based field mode")));
        }

        // test invalid getter
        try {
            assertNull(row.getField(0));
            fail();
        } catch (Throwable t) {
            assertThat(t, hasMessage(containsString("not supported in name-based field mode")));
        }
    }

    @Test
    public void testRowPositioned() {
        final Row row = Row.withPositions(RowKind.DELETE, 3);

        // test getters and setters
        row.setField(0, 42);
        row.setField(1, true);
        row.setField(2, null);
        assertThat(row.getFieldNames(false), equalTo(null));
        assertThat(row.getArity(), equalTo(3));
        assertThat(row.getKind(), equalTo(RowKind.DELETE));
        assertThat(row.getField(0), equalTo(42));
        assertThat(row.getField(1), equalTo(true));
        assertThat(row.getField(2), equalTo(null));

        // test toString
        assertThat(row.toString(), equalTo("-D[42, true, null]"));

        // test override
        row.setField(0, 13);
        row.setField(2, "Hello");
        assertThat(row.getField(0), equalTo(13));
        assertThat(row.getField(1), equalTo(true));
        assertThat(row.getField(2), equalTo("Hello"));

        // test equality
        final Row otherRow1 = Row.withPositions(RowKind.DELETE, 3);
        otherRow1.setField(0, 13);
        otherRow1.setField(1, true);
        otherRow1.setField(2, "Hello");
        assertThat(row.hashCode(), equalTo(otherRow1.hashCode()));
        assertThat(row, equalTo(otherRow1));

        // test inequality
        final Row otherRow2 = Row.withPositions(RowKind.DELETE, 3);
        otherRow2.setField(0, 13);
        otherRow2.setField(1, false); // diff here
        otherRow2.setField(2, "Hello");
        assertThat(row.hashCode(), not(equalTo(otherRow2.hashCode())));
        assertThat(row, not(equalTo(otherRow2)));

        // test clear
        row.clear();
        assertThat(row.getArity(), equalTo(3));
        assertThat(row.getFieldNames(false), equalTo(null));
        assertThat(row.toString(), equalTo("-D[null, null, null]"));

        // test invalid setter
        try {
            row.setField("a", 13);
            fail();
        } catch (Throwable t) {
            assertThat(t, hasMessage(containsString("not supported in position-based field mode")));
        }

        // test invalid getter
        try {
            assertNull(row.getField("a"));
            fail();
        } catch (Throwable t) {
            assertThat(t, hasMessage(containsString("not supported in position-based field mode")));
        }
    }

    @Test
    public void testRowNamedPositioned() {
        final LinkedHashMap<String, Integer> positionByName = new LinkedHashMap<>();
        positionByName.put("a", 0);
        positionByName.put("b", 1);
        positionByName.put("c", 2);
        final Row row =
                RowUtils.createRowWithNamedPositions(RowKind.DELETE, new Object[3], positionByName);

        // test getters and setters
        row.setField(0, 42);
        row.setField("b", true);
        row.setField(2, null);
        assertThat(row.getFieldNames(false), equalTo(null));
        assertThat(row.getFieldNames(true), contains("a", "b", "c"));
        assertThat(row.getArity(), equalTo(3));
        assertThat(row.getKind(), equalTo(RowKind.DELETE));
        assertThat(row.getField(0), equalTo(42));
        assertThat(row.getField(1), equalTo(true));
        assertThat(row.getField("c"), equalTo(null));

        // test toString
        assertThat(row.toString(), equalTo("-D[42, true, null]"));

        // test override
        row.setField("a", 13);
        row.setField(2, "Hello");
        assertThat(row.getField(0), equalTo(13));
        assertThat(row.getField("b"), equalTo(true));
        assertThat(row.getField(2), equalTo("Hello"));

        // test equality
        final Row otherRow1 = Row.withPositions(RowKind.DELETE, 3);
        otherRow1.setField(0, 13);
        otherRow1.setField(1, true);
        otherRow1.setField(2, "Hello");
        assertThat(row.hashCode(), equalTo(otherRow1.hashCode()));
        assertThat(row, equalTo(otherRow1));

        // test inequality
        final Row otherRow2 = Row.withPositions(RowKind.DELETE, 3);
        otherRow2.setField(0, 13);
        otherRow2.setField(1, false); // diff here
        otherRow2.setField(2, "Hello");
        assertThat(row.hashCode(), not(equalTo(otherRow2.hashCode())));
        assertThat(row, not(equalTo(otherRow2)));

        // test clear
        row.clear();
        assertThat(row.getArity(), equalTo(3));
        assertThat(row.getFieldNames(true), contains("a", "b", "c"));
        assertThat(row.toString(), equalTo("-D[null, null, null]"));

        // test invalid setter
        try {
            row.setField("DOES_NOT_EXIST", 13);
            fail();
        } catch (Throwable t) {
            assertThat(t, hasMessage(containsString("Unknown field name 'DOES_NOT_EXIST'")));
        }

        // test invalid getter
        try {
            assertNull(row.getField("DOES_NOT_EXIST"));
            fail();
        } catch (Throwable t) {
            assertThat(t, hasMessage(containsString("Unknown field name 'DOES_NOT_EXIST'")));
        }
    }

    @Test
    public void testRowOf() {
        final Row row1 = Row.of(1, "hello", null, Tuple2.of(2L, "hi"), true);

        final Row row2 = Row.withPositions(5);
        row2.setField(0, 1);
        row2.setField(1, "hello");
        row2.setField(2, null);
        row2.setField(3, new Tuple2<>(2L, "hi"));
        row2.setField(4, true);

        assertEquals(row1, row2);
    }

    @Test
    public void testRowCopyPositioned() {
        final Row row = Row.withPositions(5);
        row.setField(0, 1);
        row.setField(1, "hello");
        row.setField(2, null);
        row.setField(3, new Tuple2<>(2, "hi"));
        row.setField(4, "hello world");

        final Row copy = Row.copy(row);
        assertEquals(row, copy);
        assertNotSame(row, copy);
    }

    @Test
    public void testRowCopyNamed() {
        final Row row = Row.withNames();
        row.setField("a", 1);
        row.setField("b", "hello");
        row.setField("c", null);
        row.setField("d", new Tuple2<>(2, "hi"));
        row.setField("e", "hello world");

        final Row copy = Row.copy(row);
        assertEquals(row, copy);
        assertNotSame(row, copy);
    }

    @Test
    public void testRowProjectPositioned() {
        final Row row = Row.withPositions(5);
        row.setField(0, 1);
        row.setField(1, "hello");
        row.setField(2, null);
        row.setField(3, new Tuple2<>(2, "hi"));
        row.setField(4, "hello world");

        final Row projected = Row.project(row, new int[] {0, 2, 4});

        final Row expected = Row.withPositions(3);
        expected.setField(0, 1);
        expected.setField(1, null);
        expected.setField(2, "hello world");

        assertEquals(expected, projected);
    }

    @Test
    public void testRowProjectNamed() {
        final Row row = Row.withNames();
        row.setField("a", 1);
        row.setField("b", "hello");
        row.setField("c", null);
        row.setField("d", new Tuple2<>(2, "hi"));
        row.setField("e", "hello world");

        final Row projected = Row.project(row, new String[] {"a", "c", "e"});

        final Row expected = Row.withNames();
        expected.setField("a", 1);
        expected.setField("c", null);
        expected.setField("e", "hello world");

        assertEquals(expected, projected);
    }

    @Test
    public void testRowJoinPositioned() {
        Row row1 = new Row(2);
        row1.setField(0, 1);
        row1.setField(1, "hello");

        Row row2 = new Row(2);
        row2.setField(0, null);
        row2.setField(1, new Tuple2<>(2, "hi"));

        Row row3 = new Row(1);
        row3.setField(0, "hello world");

        Row joinedRow = Row.join(row1, row2, row3);

        Row expected = new Row(5);
        expected.setField(0, 1);
        expected.setField(1, "hello");
        expected.setField(2, null);
        expected.setField(3, new Tuple2<>(2, "hi"));
        expected.setField(4, "hello world");
        assertEquals(expected, joinedRow);
    }

    @Test
    public void testDeepEqualsAndHashCodePositioned() {
        final Map<String, byte[]> originalMap = new HashMap<>();
        originalMap.put("k1", new byte[] {1, 2, 3});
        originalMap.put("k2", new byte[] {3, 4, 6});

        final Row originalRow =
                Row.ofKind(
                        RowKind.INSERT,
                        true,
                        new Integer[] {1, null, 3},
                        Arrays.asList(1, null, 3),
                        originalMap,
                        Collections.emptyMap(),
                        new int[][] {{1, 2, 3}, {}, {4, 5}},
                        1.44);
        assertEquals(originalRow, originalRow);
        assertEquals(originalRow.hashCode(), originalRow.hashCode());

        {
            // no diff
            final Row row =
                    Row.ofKind(
                            RowKind.INSERT,
                            true,
                            new Integer[] {1, null, 3},
                            Arrays.asList(1, null, 3),
                            originalMap,
                            Collections.emptyMap(),
                            new int[][] {{1, 2, 3}, {}, {4, 5}},
                            1.44);
            assertEquals(row, originalRow);
            assertEquals(row.hashCode(), originalRow.hashCode());
        }

        {
            final Map<String, byte[]> map = new HashMap<>();
            map.put("k1", new byte[] {1, 2, 3});
            map.put("k2", new byte[] {3, 4, 6});

            final Row row =
                    Row.ofKind(
                            RowKind.INSERT,
                            true,
                            new Integer[] {1, null, 3, 99}, // diff here
                            Arrays.asList(1, null, 3),
                            map,
                            Collections.emptyMap(),
                            new int[][] {{1, 2, 3}, {}, {4, 5}},
                            1.44);
            assertNotEquals(row, originalRow);
            assertNotEquals(row.hashCode(), originalRow.hashCode());
        }

        {
            final Map<String, byte[]> map = new HashMap<>();
            map.put("k1", new byte[] {1, 2, 2}); // diff here
            map.put("k2", new byte[] {3, 4, 6});

            final Row row =
                    Row.ofKind(
                            RowKind.INSERT,
                            true,
                            new Integer[] {1, null, 3},
                            Arrays.asList(1, null, 3),
                            map,
                            Collections.emptyMap(),
                            new int[][] {{1, 2, 3}, {}, {4, 5}},
                            1.44);
            assertNotEquals(row, originalRow);
            assertNotEquals(row.hashCode(), originalRow.hashCode());
        }

        {
            final Map<String, byte[]> map = new HashMap<>();
            map.put("k1", new byte[] {1, 2, 3});
            map.put("k2", new byte[] {3, 4, 6});

            final Row row =
                    Row.ofKind(
                            RowKind.INSERT,
                            true,
                            new Integer[] {1, null, 3},
                            Arrays.asList(1, null, 3),
                            map,
                            Collections.emptyMap(),
                            new Integer[][] {{1, 2, 3}, {}, {4, 5}}, // diff here
                            1.44);
            assertNotEquals(row, originalRow);
            assertNotEquals(row.hashCode(), originalRow.hashCode());
        }
    }

    @Test
    public void testDeepEqualsCodeNamed() {
        final Row named = Row.withNames(RowKind.DELETE);
        named.setField("a", 12); // "b" is missing due to sparsity
        named.setField("c", true);

        final LinkedHashMap<String, Integer> positionByName = new LinkedHashMap<>();
        positionByName.put("a", 0);
        positionByName.put("b", 1);
        positionByName.put("c", 2);
        final Row namedPositioned =
                RowUtils.createRowWithNamedPositions(RowKind.DELETE, new Object[3], positionByName);
        namedPositioned.setField("a", 12);
        namedPositioned.setField("b", null);
        namedPositioned.setField("c", true);

        assertThat(named, equalTo(namedPositioned));
        assertThat(namedPositioned, equalTo(named));

        named.setField("b", "Hello");
        assertThat(named, not(equalTo(namedPositioned)));
        assertThat(namedPositioned, not(equalTo(named)));
    }

    @Test
    public void testDeepToString() {
        final Row row = Row.withNames(RowKind.UPDATE_BEFORE);
        row.setField("a", 1);
        row.setField("b", "hello");
        row.setField("c", null);
        row.setField("d", new Tuple2<>(2, "hi"));
        row.setField("e", "hello world");
        row.setField("f", new int[][] {{1}, null, {3, 4}});
        row.setField("g", new Boolean[][] {{true}, null, {false, false}});
        final Map<String, Integer[]> map = new HashMap<>();
        map.put("a", new Integer[] {1, 2, 3, 4});
        map.put("b", new Integer[] {});
        map.put("c", null);
        row.setField("h", map);

        assertThat(
                row.toString(),
                equalTo(
                        "-U{"
                                + "a=1, "
                                + "b=hello, "
                                + "c=null, "
                                + "d=(2,hi), "
                                + "e=hello world, "
                                + "f=[[1], null, [3, 4]], "
                                + "g=[[true], null, [false, false]], "
                                + "h={a=[1, 2, 3, 4], b=[], c=null}"
                                + "}"));
    }
}
