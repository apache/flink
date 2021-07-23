package org.apache.flink.table.utils;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.extraction.ExtractionUtils;

import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class ExtractionUtilsTest {

    @Test
    public void testExtractSimpleGeneric() {
        assertEquals(Optional.of(RowData.class), ExtractionUtils.extractSimpleGeneric(Parent.class, GrandChild.class, 0));
        assertEquals(Optional.empty(), ExtractionUtils.extractSimpleGeneric(Child2.class, GrandChild2.class, 0));
        assertEquals(Optional.of(RowData.class), ExtractionUtils.extractSimpleGeneric(Parent.class, Child2.class, 0));
        assertEquals(Optional.of(RowData.class), ExtractionUtils.extractSimpleGeneric(Child.class, GrandChild.class, 0));
        assertEquals(Optional.empty(), ExtractionUtils.extractSimpleGeneric(Parent.class, Child.class, 0));
        assertEquals(Optional.of(RowData.class), ExtractionUtils.extractSimpleGeneric(Parent.class, GrandChild3.class, 0));
    }

    private static class Parent<T> {}
    private static class Child<T> extends Parent<T> {}
    private static class Child2 extends Parent<RowData> {}
    private static class Child3<T, P> extends Parent<T> {}
    private static class GrandChild extends Child<RowData> {}
    private static class GrandChild2 extends Child2 {}
    private static class GrandChild3 extends Child3<RowData, Boolean> {}
}
