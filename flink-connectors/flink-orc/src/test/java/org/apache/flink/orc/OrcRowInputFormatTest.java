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

package org.apache.flink.orc;

import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;
import org.junit.After;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link OrcRowInputFormat}.
 *
 */
public class OrcRowInputFormatTest {

	private OrcRowInputFormat rowOrcInputFormat;

	@After
	public void tearDown() throws IOException {
		if (rowOrcInputFormat != null) {
			rowOrcInputFormat.close();
			rowOrcInputFormat.closeInputFormat();
		}
		rowOrcInputFormat = null;
	}

	private static final String TEST_FILE_FLAT = "test-data-flat.orc";
	private static final String TEST_SCHEMA_FLAT =
		"struct<_col0:int,_col1:string,_col2:string,_col3:string,_col4:int,_col5:string,_col6:int,_col7:int,_col8:int>";

	private static final String TEST_FILE_NESTED = "test-data-nested.orc";
	private static final String TEST_SCHEMA_NESTED =
		"struct<" +
			"boolean1:boolean," +
			"byte1:tinyint," +
			"short1:smallint," +
			"int1:int," +
			"long1:bigint," +
			"float1:float," +
			"double1:double," +
			"bytes1:binary," +
			"string1:string," +
			"middle:struct<" +
				"list:array<" +
					"struct<" +
						"int1:int," +
						"string1:string" +
					">" +
				">" +
			">," +
			"list:array<" +
				"struct<" +
					"int1:int," +
					"string1:string" +
				">" +
			">," +
			"map:map<" +
				"string," +
				"struct<" +
					"int1:int," +
					"string1:string" +
				">" +
			">" +
		">";

	private static final String TEST_FILE_TIMETYPES = "test-data-timetypes.orc";
	private static final String TEST_SCHEMA_TIMETYPES = "struct<time:timestamp,date:date>";

	private static final String TEST_FILE_DECIMAL = "test-data-decimal.orc";
	private static final String TEST_SCHEMA_DECIMAL = "struct<_col0:decimal(10,5)>";

	private static final String TEST_FILE_NESTEDLIST = "test-data-nestedlist.orc";
	private static final String TEST_SCHEMA_NESTEDLIST = "struct<mylist1:array<array<struct<mylong1:bigint>>>>";

	@Test(expected = FileNotFoundException.class)
	public void testInvalidPath() throws IOException{
		rowOrcInputFormat =
			new OrcRowInputFormat("/does/not/exist", TEST_SCHEMA_FLAT, new Configuration());
		rowOrcInputFormat.openInputFormat();
		FileInputSplit[] inputSplits = rowOrcInputFormat.createInputSplits(1);
		rowOrcInputFormat.open(inputSplits[0]);
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testInvalidProjection1() throws IOException{
		rowOrcInputFormat =
			new OrcRowInputFormat(getPath(TEST_FILE_FLAT), TEST_SCHEMA_FLAT, new Configuration());
		int[] projectionMask = {1, 2, 3, -1};
		rowOrcInputFormat.selectFields(projectionMask);
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testInvalidProjection2() throws IOException{
		rowOrcInputFormat =
			new OrcRowInputFormat(getPath(TEST_FILE_FLAT), TEST_SCHEMA_FLAT, new Configuration());
		int[] projectionMask = {1, 2, 3, 9};
		rowOrcInputFormat.selectFields(projectionMask);
	}

	@Test
	public void testProjectionMaskNested() throws IOException{
		rowOrcInputFormat =
			new OrcRowInputFormat(getPath(TEST_FILE_NESTED), TEST_SCHEMA_NESTED, new Configuration());

		OrcRowInputFormat spy = spy(rowOrcInputFormat);

		// mock options to check configuration of ORC reader
		Reader.Options options = new Reader.Options();
		doReturn(options).when(spy).getOptions(any());

		spy.selectFields(9, 11, 2);
		spy.openInputFormat();
		FileInputSplit[] splits = spy.createInputSplits(1);
		spy.open(splits[0]);

		// top-level struct is false
		boolean[] expected = new boolean[]{
			false, // top level
			false, false, // flat fields 0, 1 are out
			true, // flat field 2 is in
			false, false, false, false, false, false, // flat fields 3, 4, 5, 6, 7, 8 are out
			true, true, true, true, true, // nested field 9 is in
			false, false, false, false, // nested field 10 is out
			true, true, true, true, true}; // nested field 11 is in
		assertArrayEquals(expected, options.getInclude());
	}

	@Test
	public void testSplitStripesGivenSplits() throws IOException {
		rowOrcInputFormat =
			new OrcRowInputFormat(getPath(TEST_FILE_FLAT), TEST_SCHEMA_FLAT, new Configuration());

		OrcRowInputFormat spy = spy(rowOrcInputFormat);

		// mock options to check configuration of ORC reader
		Reader.Options options = spy(new Reader.Options());
		doReturn(options).when(spy).getOptions(any());

		FileInputSplit[] splits = spy.createInputSplits(3);

		spy.openInputFormat();
		spy.open(splits[0]);
		verify(options).range(eq(3L), eq(137005L));
		spy.open(splits[1]);
		verify(options).range(eq(137008L), eq(136182L));
		spy.open(splits[2]);
		verify(options).range(eq(273190L), eq(123633L));
	}

	@Test
	public void testSplitStripesCustomSplits() throws IOException {
		rowOrcInputFormat =
			new OrcRowInputFormat(getPath(TEST_FILE_FLAT), TEST_SCHEMA_FLAT, new Configuration());

		OrcRowInputFormat spy = spy(rowOrcInputFormat);

		// mock list of stripes
		List<StripeInformation> stripes = new ArrayList<>();
		StripeInformation stripe1 = mock(StripeInformation.class);
		when(stripe1.getOffset()).thenReturn(10L);
		when(stripe1.getLength()).thenReturn(90L);
		StripeInformation stripe2 = mock(StripeInformation.class);
		when(stripe2.getOffset()).thenReturn(100L);
		when(stripe2.getLength()).thenReturn(100L);
		StripeInformation stripe3 = mock(StripeInformation.class);
		when(stripe3.getOffset()).thenReturn(200L);
		when(stripe3.getLength()).thenReturn(100L);
		StripeInformation stripe4 = mock(StripeInformation.class);
		when(stripe4.getOffset()).thenReturn(300L);
		when(stripe4.getLength()).thenReturn(100L);
		StripeInformation stripe5 = mock(StripeInformation.class);
		when(stripe5.getOffset()).thenReturn(400L);
		when(stripe5.getLength()).thenReturn(100L);
		stripes.add(stripe1);
		stripes.add(stripe2);
		stripes.add(stripe3);
		stripes.add(stripe4);
		stripes.add(stripe5);
		doReturn(stripes).when(spy).getStripes(any());

		// mock options to check configuration of ORC reader
		Reader.Options options = spy(new Reader.Options());
		doReturn(options).when(spy).getOptions(any());

		spy.openInputFormat();
		// split ranging 2 stripes
		spy.open(new FileInputSplit(0, new Path(getPath(TEST_FILE_FLAT)), 0, 150, new String[]{}));
		verify(options).range(eq(10L), eq(190L));
		// split ranging 0 stripes
		spy.open(new FileInputSplit(1, new Path(getPath(TEST_FILE_FLAT)), 150, 10, new String[]{}));
		verify(options).range(eq(0L), eq(0L));
		// split ranging 1 stripe
		spy.open(new FileInputSplit(2, new Path(getPath(TEST_FILE_FLAT)), 160, 41, new String[]{}));
		verify(options).range(eq(200L), eq(100L));
		// split ranging 2 stripe
		spy.open(new FileInputSplit(3, new Path(getPath(TEST_FILE_FLAT)), 201, 299, new String[]{}));
		verify(options).range(eq(300L), eq(200L));
	}

	@Test
	public void testProducedType() throws IOException {
		rowOrcInputFormat =
			new OrcRowInputFormat(getPath(TEST_FILE_NESTED), TEST_SCHEMA_NESTED, new Configuration());

		assertTrue(rowOrcInputFormat.getProducedType() instanceof RowTypeInfo);
		RowTypeInfo producedType = (RowTypeInfo) rowOrcInputFormat.getProducedType();

		assertArrayEquals(
			new TypeInformation[]{
				// primitives
				Types.BOOLEAN, Types.BYTE, Types.SHORT, Types.INT, Types.LONG, Types.FLOAT, Types.DOUBLE,
				// binary
				PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
				// string
				Types.STRING,
				// struct
				Types.ROW_NAMED(
					new String[]{"list"},
					ObjectArrayTypeInfo.getInfoFor(
						Types.ROW_NAMED(new String[]{"int1", "string1"}, Types.INT, Types.STRING))),
				// list
				ObjectArrayTypeInfo.getInfoFor(
					Types.ROW_NAMED(new String[]{"int1", "string1"}, Types.INT, Types.STRING)),
				// map
				new MapTypeInfo<>(Types.STRING, Types.ROW_NAMED(new String[]{"int1", "string1"}, Types.INT, Types.STRING))
			},
			producedType.getFieldTypes());
		assertArrayEquals(
			new String[]{"boolean1", "byte1", "short1", "int1", "long1", "float1", "double1", "bytes1", "string1", "middle", "list", "map"},
			producedType.getFieldNames());
	}

	@Test
	public void testProducedTypeWithProjection() throws IOException {
		rowOrcInputFormat =
			new OrcRowInputFormat(getPath(TEST_FILE_NESTED), TEST_SCHEMA_NESTED, new Configuration());

		rowOrcInputFormat.selectFields(9, 3, 7, 10);

		assertTrue(rowOrcInputFormat.getProducedType() instanceof RowTypeInfo);
		RowTypeInfo producedType = (RowTypeInfo) rowOrcInputFormat.getProducedType();

		assertArrayEquals(
			new TypeInformation[]{
				// struct
				Types.ROW_NAMED(
					new String[]{"list"},
					ObjectArrayTypeInfo.getInfoFor(
						Types.ROW_NAMED(new String[]{"int1", "string1"}, Types.INT, Types.STRING))),
				// int
				Types.INT,
				// binary
				PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
				// list
				ObjectArrayTypeInfo.getInfoFor(
					Types.ROW_NAMED(new String[]{"int1", "string1"}, Types.INT, Types.STRING))
			},
			producedType.getFieldTypes());
		assertArrayEquals(
			new String[]{"middle", "int1", "bytes1", "list"},
			producedType.getFieldNames());
	}

	@Test
	public void testSerialization() throws Exception {
		rowOrcInputFormat =
			new OrcRowInputFormat(getPath(TEST_FILE_FLAT), TEST_SCHEMA_FLAT, new Configuration());

		rowOrcInputFormat.selectFields(0, 4, 1);
		rowOrcInputFormat.addPredicate(
			new OrcRowInputFormat.Equals("_col1", PredicateLeaf.Type.STRING, "M"));

		byte[] bytes = InstantiationUtil.serializeObject(rowOrcInputFormat);
		OrcRowInputFormat copy = InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());

		FileInputSplit[] splits = copy.createInputSplits(1);
		copy.openInputFormat();
		copy.open(splits[0]);
		assertFalse(copy.reachedEnd());
		Row row = copy.nextRecord(null);

		assertNotNull(row);
		assertEquals(3, row.getArity());
		// check first row
		assertEquals(1, row.getField(0));
		assertEquals(500, row.getField(1));
		assertEquals("M", row.getField(2));
	}

	@Test
	public void testNumericBooleanStringPredicates() throws Exception {
		rowOrcInputFormat =
			new OrcRowInputFormat(getPath(TEST_FILE_NESTED), TEST_SCHEMA_NESTED, new Configuration());

		rowOrcInputFormat.selectFields(0, 1, 2, 3, 4, 5, 6, 8);

		// boolean pred
		rowOrcInputFormat.addPredicate(
			new OrcRowInputFormat.Equals("boolean1", PredicateLeaf.Type.BOOLEAN, false));
		// boolean pred
		rowOrcInputFormat.addPredicate(
			new OrcRowInputFormat.LessThan("byte1", PredicateLeaf.Type.LONG, 1));
		// boolean pred
		rowOrcInputFormat.addPredicate(
			new OrcRowInputFormat.LessThanEquals("short1", PredicateLeaf.Type.LONG, 1024));
		// boolean pred
		rowOrcInputFormat.addPredicate(
			new OrcRowInputFormat.Between("int1", PredicateLeaf.Type.LONG, -1, 65536));
		// boolean pred
		rowOrcInputFormat.addPredicate(
			new OrcRowInputFormat.Equals("long1", PredicateLeaf.Type.LONG, 9223372036854775807L));
		// boolean pred
		rowOrcInputFormat.addPredicate(
			new OrcRowInputFormat.Equals("float1", PredicateLeaf.Type.FLOAT, 1.0));
		// boolean pred
		rowOrcInputFormat.addPredicate(
			new OrcRowInputFormat.Equals("double1", PredicateLeaf.Type.FLOAT, -15.0));
		// boolean pred
		rowOrcInputFormat.addPredicate(
			new OrcRowInputFormat.IsNull("string1", PredicateLeaf.Type.STRING));
		// boolean pred
		rowOrcInputFormat.addPredicate(
			new OrcRowInputFormat.Equals("string1", PredicateLeaf.Type.STRING, "hello"));

		FileInputSplit[] splits = rowOrcInputFormat.createInputSplits(1);
		rowOrcInputFormat.openInputFormat();

		// mock options to check configuration of ORC reader
		OrcRowInputFormat spy = spy(rowOrcInputFormat);
		Reader.Options options = new Reader.Options();
		doReturn(options).when(spy).getOptions(any());

		spy.openInputFormat();
		spy.open(splits[0]);

		// verify predicate configuration
		SearchArgument sarg = options.getSearchArgument();
		assertNotNull(sarg);
		assertEquals("(and leaf-0 leaf-1 leaf-2 leaf-3 leaf-4 leaf-5 leaf-6 leaf-7 leaf-8)", sarg.getExpression().toString());
		assertEquals(9, sarg.getLeaves().size());
		List<PredicateLeaf> leaves = sarg.getLeaves();
		assertEquals("(EQUALS boolean1 false)", leaves.get(0).toString());
		assertEquals("(LESS_THAN byte1 1)", leaves.get(1).toString());
		assertEquals("(LESS_THAN_EQUALS short1 1024)", leaves.get(2).toString());
		assertEquals("(BETWEEN int1 -1 65536)", leaves.get(3).toString());
		assertEquals("(EQUALS long1 9223372036854775807)", leaves.get(4).toString());
		assertEquals("(EQUALS float1 1.0)", leaves.get(5).toString());
		assertEquals("(EQUALS double1 -15.0)", leaves.get(6).toString());
		assertEquals("(IS_NULL string1)", leaves.get(7).toString());
		assertEquals("(EQUALS string1 hello)", leaves.get(8).toString());
	}

	@Test
	public void testTimePredicates() throws Exception {
		rowOrcInputFormat =
			new OrcRowInputFormat(getPath(TEST_FILE_TIMETYPES), TEST_SCHEMA_TIMETYPES, new Configuration());

		rowOrcInputFormat.addPredicate(
			// OR
			new OrcRowInputFormat.Or(
				// timestamp pred
				new OrcRowInputFormat.Equals("time", PredicateLeaf.Type.TIMESTAMP, Timestamp.valueOf("1900-05-05 12:34:56.100")),
				// date pred
				new OrcRowInputFormat.Equals("date", PredicateLeaf.Type.DATE, Date.valueOf("1900-12-25")))
			);

		FileInputSplit[] splits = rowOrcInputFormat.createInputSplits(1);
		rowOrcInputFormat.openInputFormat();

		// mock options to check configuration of ORC reader
		OrcRowInputFormat spy = spy(rowOrcInputFormat);
		Reader.Options options = new Reader.Options();
		doReturn(options).when(spy).getOptions(any());

		spy.openInputFormat();
		spy.open(splits[0]);

		// verify predicate configuration
		SearchArgument sarg = options.getSearchArgument();
		assertNotNull(sarg);
		assertEquals("(or leaf-0 leaf-1)", sarg.getExpression().toString());
		assertEquals(2, sarg.getLeaves().size());
		List<PredicateLeaf> leaves = sarg.getLeaves();
		assertEquals("(EQUALS time 1900-05-05 12:34:56.1)", leaves.get(0).toString());
		assertEquals("(EQUALS date 1900-12-25)", leaves.get(1).toString());
	}

	@Test
	public void testDecimalPredicate() throws Exception {
		rowOrcInputFormat =
			new OrcRowInputFormat(getPath(TEST_FILE_DECIMAL), TEST_SCHEMA_DECIMAL, new Configuration());

		rowOrcInputFormat.addPredicate(
			new OrcRowInputFormat.Not(
				// decimal pred
				new OrcRowInputFormat.Equals("_col0", PredicateLeaf.Type.DECIMAL, BigDecimal.valueOf(-1000.5))));

		FileInputSplit[] splits = rowOrcInputFormat.createInputSplits(1);
		rowOrcInputFormat.openInputFormat();

		// mock options to check configuration of ORC reader
		OrcRowInputFormat spy = spy(rowOrcInputFormat);
		Reader.Options options = new Reader.Options();
		doReturn(options).when(spy).getOptions(any());

		spy.openInputFormat();
		spy.open(splits[0]);

		// verify predicate configuration
		SearchArgument sarg = options.getSearchArgument();
		assertNotNull(sarg);
		assertEquals("(not leaf-0)", sarg.getExpression().toString());
		assertEquals(1, sarg.getLeaves().size());
		List<PredicateLeaf> leaves = sarg.getLeaves();
		assertEquals("(EQUALS _col0 -1000.5)", leaves.get(0).toString());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testPredicateWithInvalidColumn() throws Exception {
		rowOrcInputFormat =
			new OrcRowInputFormat(getPath(TEST_FILE_NESTED), TEST_SCHEMA_NESTED, new Configuration());

		rowOrcInputFormat.addPredicate(
			new OrcRowInputFormat.Equals("unknown", PredicateLeaf.Type.LONG, 42));
	}

	@Test
	public void testReadNestedFile() throws IOException{
		rowOrcInputFormat = new OrcRowInputFormat(getPath(TEST_FILE_NESTED), TEST_SCHEMA_NESTED, new Configuration());

		FileInputSplit[] splits = rowOrcInputFormat.createInputSplits(1);
		assertEquals(1, splits.length);
		rowOrcInputFormat.openInputFormat();
		rowOrcInputFormat.open(splits[0]);

		assertFalse(rowOrcInputFormat.reachedEnd());
		Row row = rowOrcInputFormat.nextRecord(null);

		// validate first row
		assertNotNull(row);
		assertEquals(12, row.getArity());
		assertEquals(false, row.getField(0));
		assertEquals((byte) 1, row.getField(1));
		assertEquals((short) 1024, row.getField(2));
		assertEquals(65536, row.getField(3));
		assertEquals(9223372036854775807L, row.getField(4));
		assertEquals(1.0f, row.getField(5));
		assertEquals(-15.0d, row.getField(6));
		assertArrayEquals(new byte[]{0, 1, 2, 3, 4}, (byte[]) row.getField(7));
		assertEquals("hi", row.getField(8));
		// check nested field
		assertTrue(row.getField(9) instanceof Row);
		Row nested1 = (Row) row.getField(9);
		assertEquals(1, nested1.getArity());
		assertTrue(nested1.getField(0) instanceof Object[]);
		Object[] nestedList1 = (Object[]) nested1.getField(0);
		assertEquals(2, nestedList1.length);
		assertEquals(Row.of(1, "bye"), nestedList1[0]);
		assertEquals(Row.of(2, "sigh"), nestedList1[1]);
		// check list
		assertTrue(row.getField(10) instanceof Object[]);
		Object[] list1 = (Object[]) row.getField(10);
		assertEquals(2, list1.length);
		assertEquals(Row.of(3, "good"), list1[0]);
		assertEquals(Row.of(4, "bad"), list1[1]);
		// check map
		assertTrue(row.getField(11) instanceof HashMap);
		HashMap map1 = (HashMap) row.getField(11);
		assertEquals(0, map1.size());

		// read second row
		assertFalse(rowOrcInputFormat.reachedEnd());
		row = rowOrcInputFormat.nextRecord(null);

		// validate second row
		assertNotNull(row);
		assertEquals(12, row.getArity());
		assertEquals(true, row.getField(0));
		assertEquals((byte) 100, row.getField(1));
		assertEquals((short) 2048, row.getField(2));
		assertEquals(65536, row.getField(3));
		assertEquals(9223372036854775807L, row.getField(4));
		assertEquals(2.0f, row.getField(5));
		assertEquals(-5.0d, row.getField(6));
		assertArrayEquals(new byte[]{}, (byte[]) row.getField(7));
		assertEquals("bye", row.getField(8));
		// check nested field
		assertTrue(row.getField(9) instanceof Row);
		Row nested2 = (Row) row.getField(9);
		assertEquals(1, nested2.getArity());
		assertTrue(nested2.getField(0) instanceof Object[]);
		Object[] nestedList2 = (Object[]) nested2.getField(0);
		assertEquals(2, nestedList2.length);
		assertEquals(Row.of(1, "bye"), nestedList2[0]);
		assertEquals(Row.of(2, "sigh"), nestedList2[1]);
		// check list
		assertTrue(row.getField(10) instanceof Object[]);
		Object[] list2 = (Object[]) row.getField(10);
		assertEquals(3, list2.length);
		assertEquals(Row.of(100000000, "cat"), list2[0]);
		assertEquals(Row.of(-100000, "in"), list2[1]);
		assertEquals(Row.of(1234, "hat"), list2[2]);
		// check map
		assertTrue(row.getField(11) instanceof HashMap);
		HashMap map = (HashMap) row.getField(11);
		assertEquals(2, map.size());
		assertEquals(Row.of(5, "chani"), map.get("chani"));
		assertEquals(Row.of(1, "mauddib"), map.get("mauddib"));

		assertTrue(rowOrcInputFormat.reachedEnd());
	}

	@Test
	public void testReadTimeTypeFile() throws IOException{
		rowOrcInputFormat = new OrcRowInputFormat(getPath(TEST_FILE_TIMETYPES), TEST_SCHEMA_TIMETYPES, new Configuration());

		FileInputSplit[] splits = rowOrcInputFormat.createInputSplits(1);
		assertEquals(1, splits.length);
		rowOrcInputFormat.openInputFormat();
		rowOrcInputFormat.open(splits[0]);

		assertFalse(rowOrcInputFormat.reachedEnd());
		Row row = rowOrcInputFormat.nextRecord(null);

		// validate first row
		assertNotNull(row);
		assertEquals(2, row.getArity());
		assertEquals(Timestamp.valueOf("1900-05-05 12:34:56.1"), row.getField(0));
		assertEquals(Date.valueOf("1900-12-25"), row.getField(1));

		// check correct number of rows
		long cnt = 1;
		while (!rowOrcInputFormat.reachedEnd()) {
			assertNotNull(rowOrcInputFormat.nextRecord(null));
			cnt++;
		}
		assertEquals(70000, cnt);
	}

	@Test
	public void testReadDecimalTypeFile() throws IOException{
		rowOrcInputFormat = new OrcRowInputFormat(getPath(TEST_FILE_DECIMAL), TEST_SCHEMA_DECIMAL, new Configuration());

		FileInputSplit[] splits = rowOrcInputFormat.createInputSplits(1);
		assertEquals(1, splits.length);
		rowOrcInputFormat.openInputFormat();
		rowOrcInputFormat.open(splits[0]);

		assertFalse(rowOrcInputFormat.reachedEnd());
		Row row = rowOrcInputFormat.nextRecord(null);

		// validate first row
		assertNotNull(row);
		assertEquals(1, row.getArity());
		assertEquals(BigDecimal.valueOf(-1000.5d), row.getField(0));

		// check correct number of rows
		long cnt = 1;
		while (!rowOrcInputFormat.reachedEnd()) {
			assertNotNull(rowOrcInputFormat.nextRecord(null));
			cnt++;
		}
		assertEquals(6000, cnt);
	}

	@Test
	public void testReadNestedListFile() throws Exception {
		rowOrcInputFormat = new OrcRowInputFormat(getPath(TEST_FILE_NESTEDLIST), TEST_SCHEMA_NESTEDLIST, new Configuration());

		FileInputSplit[] splits = rowOrcInputFormat.createInputSplits(1);
		assertEquals(1, splits.length);
		rowOrcInputFormat.openInputFormat();
		rowOrcInputFormat.open(splits[0]);

		assertFalse(rowOrcInputFormat.reachedEnd());

		Row row = null;
		long cnt = 0;

		// read all rows
		while (!rowOrcInputFormat.reachedEnd()) {

			row = rowOrcInputFormat.nextRecord(row);
			assertEquals(1, row.getArity());

			// outer list
			Object[] list = (Object[]) row.getField(0);
			assertEquals(1, list.length);

			// nested list of rows
			Row[] nestedRows = (Row[]) list[0];
			assertEquals(1, nestedRows.length);
			assertEquals(1, nestedRows[0].getArity());

			// verify list value
			assertEquals(cnt, nestedRows[0].getField(0));
			cnt++;
		}
		// number of rows in file
		assertEquals(100, cnt);
	}

	@Test
	public void testReadWithProjection() throws IOException{
		rowOrcInputFormat = new OrcRowInputFormat(getPath(TEST_FILE_NESTED), TEST_SCHEMA_NESTED, new Configuration());

		rowOrcInputFormat.selectFields(7, 0, 10, 8);

		FileInputSplit[] splits = rowOrcInputFormat.createInputSplits(1);
		assertEquals(1, splits.length);
		rowOrcInputFormat.openInputFormat();
		rowOrcInputFormat.open(splits[0]);

		assertFalse(rowOrcInputFormat.reachedEnd());
		Row row = rowOrcInputFormat.nextRecord(null);

		// validate first row
		assertNotNull(row);
		assertEquals(4, row.getArity());
		// check binary
		assertArrayEquals(new byte[]{0, 1, 2, 3, 4}, (byte[]) row.getField(0));
		// check boolean
		assertEquals(false, row.getField(1));
		// check list
		assertTrue(row.getField(2) instanceof Object[]);
		Object[] list1 = (Object[]) row.getField(2);
		assertEquals(2, list1.length);
		assertEquals(Row.of(3, "good"), list1[0]);
		assertEquals(Row.of(4, "bad"), list1[1]);
		// check string
		assertEquals("hi", row.getField(3));

		// check that there is a second row with four fields
		assertFalse(rowOrcInputFormat.reachedEnd());
		row = rowOrcInputFormat.nextRecord(null);
		assertNotNull(row);
		assertEquals(4, row.getArity());
		assertTrue(rowOrcInputFormat.reachedEnd());
	}

	@Test
	public void testReadFileInSplits() throws IOException{

		rowOrcInputFormat = new OrcRowInputFormat(getPath(TEST_FILE_FLAT), TEST_SCHEMA_FLAT, new Configuration());
		rowOrcInputFormat.selectFields(0, 1);

		FileInputSplit[] splits = rowOrcInputFormat.createInputSplits(4);
		assertEquals(4, splits.length);
		rowOrcInputFormat.openInputFormat();

		long cnt = 0;
		// read all splits
		for (FileInputSplit split : splits) {

			// open split
			rowOrcInputFormat.open(split);
			// read and count all rows
			while (!rowOrcInputFormat.reachedEnd()) {
				assertNotNull(rowOrcInputFormat.nextRecord(null));
				cnt++;
			}
		}
		// check that all rows have been read
		assertEquals(1920800, cnt);
	}

	@Test
	public void testReadFileWithFilter() throws IOException{

		rowOrcInputFormat = new OrcRowInputFormat(getPath(TEST_FILE_FLAT), TEST_SCHEMA_FLAT, new Configuration());
		rowOrcInputFormat.selectFields(0, 1);

		// read head and tail of file
		rowOrcInputFormat.addPredicate(
			new OrcRowInputFormat.Or(
				new OrcRowInputFormat.LessThan("_col0", PredicateLeaf.Type.LONG, 10L),
				new OrcRowInputFormat.Not(
					new OrcRowInputFormat.LessThanEquals("_col0", PredicateLeaf.Type.LONG, 1920000L))
			));
		rowOrcInputFormat.addPredicate(
			new OrcRowInputFormat.Equals("_col1", PredicateLeaf.Type.STRING, "M"));

		FileInputSplit[] splits = rowOrcInputFormat.createInputSplits(1);
		assertEquals(1, splits.length);
		rowOrcInputFormat.openInputFormat();

		// open split
		rowOrcInputFormat.open(splits[0]);

		// read and count all rows
		long cnt = 0;
		while (!rowOrcInputFormat.reachedEnd()) {
			assertNotNull(rowOrcInputFormat.nextRecord(null));
			cnt++;
		}
		// check that only the first and last stripes of the file have been read.
		// Each stripe has 5000 rows, except the last which has 800 rows.
		assertEquals(5800, cnt);
	}

	@Test
	public void testReadFileWithEvolvedSchema() throws IOException{

		rowOrcInputFormat = new OrcRowInputFormat(
			getPath(TEST_FILE_FLAT),
			"struct<_col0:int,_col1:string,_col4:string,_col3:string>", // previous version of schema
			new Configuration());
		rowOrcInputFormat.selectFields(3, 0, 2);

		rowOrcInputFormat.addPredicate(
			new OrcRowInputFormat.LessThan("_col0", PredicateLeaf.Type.LONG, 10L));

		FileInputSplit[] splits = rowOrcInputFormat.createInputSplits(1);
		assertEquals(1, splits.length);
		rowOrcInputFormat.openInputFormat();

		// open split
		rowOrcInputFormat.open(splits[0]);

		// read and validate first row
		assertFalse(rowOrcInputFormat.reachedEnd());
		Row row = rowOrcInputFormat.nextRecord(null);
		assertNotNull(row);
		assertEquals(3, row.getArity());
		assertEquals("Primary", row.getField(0));
		assertEquals(1, row.getField(1));
		assertEquals("M", row.getField(2));

		// read and count remaining rows
		long cnt = 1;
		while (!rowOrcInputFormat.reachedEnd()) {
			assertNotNull(rowOrcInputFormat.nextRecord(null));
			cnt++;
		}
		// check that only the first and last stripes of the file have been read.
		// Each stripe has 5000 rows, except the last which has 800 rows.
		assertEquals(5000, cnt);
	}

	private String getPath(String fileName) {
		return getClass().getClassLoader().getResource(fileName).getPath();
	}
}
