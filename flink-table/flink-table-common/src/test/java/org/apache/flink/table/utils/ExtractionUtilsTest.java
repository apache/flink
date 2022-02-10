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

package org.apache.flink.table.utils;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.extraction.ExtractionUtils;

import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;

/** Tests for {@link org.apache.flink.table.types.extraction.ExtractionUtils}. */
public class ExtractionUtilsTest {

    @Test
    public void testExtractSimpleGeneric() {
        assertEquals(
                Optional.of(RowData.class),
                ExtractionUtils.extractSimpleGeneric(Parent.class, GrandChild.class, 0));
        assertEquals(
                Optional.empty(),
                ExtractionUtils.extractSimpleGeneric(Child2.class, GrandChild2.class, 0));
        assertEquals(
                Optional.of(RowData.class),
                ExtractionUtils.extractSimpleGeneric(Parent.class, Child2.class, 0));
        assertEquals(
                Optional.of(RowData.class),
                ExtractionUtils.extractSimpleGeneric(Child.class, GrandChild.class, 0));
        assertEquals(
                Optional.empty(),
                ExtractionUtils.extractSimpleGeneric(Parent.class, Child.class, 0));
        assertEquals(
                Optional.of(RowData.class),
                ExtractionUtils.extractSimpleGeneric(Parent.class, GrandChild3.class, 0));
    }

    private static class Parent<T> {}

    private static class Child<T> extends Parent<T> {}

    private static class Child2 extends Parent<RowData> {}

    private static class Child3<T, P> extends Parent<T> {}

    private static class GrandChild extends Child<RowData> {}

    private static class GrandChild2 extends Child2 {}

    private static class GrandChild3 extends Child3<RowData, Boolean> {}
}
