package org.apache.flink.table.data;

import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Created by zoudan on 2020/6/12.
 */
public class Test1 {
	@Test
	public void Test() {
		Row[] rows1 = new Row[] {Row.of("a", 1), Row.of("b", 2)};
		Row[] rows2 = new Row[] {Row.of("a", 1), Row.of("b", 2)};
		Row[] rows3 = new Row[] {Row.of("1"), Row.of("b", 2)};

		assertArrayEquals(rows1, rows2);
		assertTrue(Arrays.equals(rows1, rows2));
		assertFalse(Arrays.deepEquals(rows1, rows2));
		assertFalse(Arrays.equals(rows1, rows2));
	}
}
