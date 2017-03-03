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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.types.Row;

import org.apache.hadoop.conf.Configuration;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for the {@link RowOrcInputFormat}.
 */

public class RowOrcInputFormatTest {

	private RowOrcInputFormat rowOrcInputFormat;

	@After
	public void tearDown() throws IOException {
		if (rowOrcInputFormat != null) {
			rowOrcInputFormat.close();
			rowOrcInputFormat.closeInputFormat();
		}
		rowOrcInputFormat = null;
	}

	private final URL test1URL = getClass().getClassLoader().getResource("TestOrcFile.test1.orc");

	private static final String TEST1_SCHEMA = "struct<boolean1:boolean,byte1:tinyint,short1:smallint,int1:int," +
		"long1:bigint,float1:float,double1:double,bytes1:binary,string1:string," +
		"middle:struct<list:array<struct<int1:int,string1:string>>>," +
		"list:array<struct<int1:int,string1:string>>," +
		"map:map<string,struct<int1:int,string1:string>>>";

	private static final String[] TEST1_DATA = new String[] {
		"false,1,1024,65536,9223372036854775807,1.0,-15.0,[0, 1, 2, 3, 4],hi,[1,bye, 2,sigh],[3,good, 4,bad],{}",
		"true,100,2048,65536,9223372036854775807,2.0,-5.0,[],bye,[1,bye, 2,sigh]," +
			"[100000000,cat, -100000,in, 1234,hat],{chani=5,chani, mauddib=1,mauddib}" };

	private static final String[] TEST1_PROJECTED_DATA = new String[] {
		"{},[3,good, 4,bad],[1,bye, 2,sigh],hi,[0, 1, 2, 3, 4],-15.0,1.0,9223372036854775807,65536,1024,1,false",
		"{chani=5,chani, mauddib=1,mauddib},[100000000,cat, -100000,in, 1234,hat],[1,bye, 2,sigh],bye," +
			"[],-5.0,2.0,9223372036854775807,65536,2048,100,true" };

	private static final String TEST1_INVALID_SCHEMA = "struct<boolean1:int,byte1:tinyint,short1:smallint,int1:int," +
		"long1:bigint,float1:float,double1:double,bytes1:binary,string1:string," +
		"middle:struct<list:array<struct<int1:int,string1:string>>>," +
		"list:array<struct<int1:int,string1:string>>," +
		"map:map<string,struct<int1:int,string1:string>>>";

	@Test(expected = FileNotFoundException.class)
	public void testInvalidPath() throws IOException{

		rowOrcInputFormat = new RowOrcInputFormat("TestOrcFile.test2.orc", TEST1_SCHEMA, new Configuration());
		rowOrcInputFormat.openInputFormat();
		FileInputSplit[] inputSplits = rowOrcInputFormat.createInputSplits(1);
		rowOrcInputFormat.open(inputSplits[0]);

	}

	@Test(expected = RuntimeException.class)
	public void testInvalidSchema() throws IOException{

		assert(test1URL != null);
		rowOrcInputFormat = new RowOrcInputFormat(test1URL.getPath(), TEST1_INVALID_SCHEMA, new Configuration());
		rowOrcInputFormat.openInputFormat();
		FileInputSplit[] inputSplits = rowOrcInputFormat.createInputSplits(1);
		rowOrcInputFormat.open(inputSplits[0]);

	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testInvalidProjection() throws IOException{

		assert(test1URL != null);
		rowOrcInputFormat = new RowOrcInputFormat(test1URL.getPath(), TEST1_SCHEMA, new Configuration());
		int[] projectionMask = {14};
		rowOrcInputFormat.setFieldMapping(projectionMask);
	}

	@Test
	public void testMajorDataTypes() throws IOException{

		// test for boolean,byte,short,int,long,float,double,bytes,string,struct,list,map
		assert(test1URL != null);
		rowOrcInputFormat = new RowOrcInputFormat(test1URL.getPath(), TEST1_SCHEMA, new Configuration());
		rowOrcInputFormat.openInputFormat();
		FileInputSplit[] inputSplits = rowOrcInputFormat.createInputSplits(1);

		Assert.assertEquals(inputSplits.length, 1);

		Row row = null;
		int count = 0;
		for (FileInputSplit split : inputSplits) {
			rowOrcInputFormat.open(split);
			while (!rowOrcInputFormat.reachedEnd()) {
				row = rowOrcInputFormat.nextRecord(row);
				Assert.assertEquals(row.toString(), TEST1_DATA[count++]);
			}
		}
	}

	@Test
	public void testProjection() throws IOException{

		assert(test1URL != null);
		rowOrcInputFormat = new RowOrcInputFormat(test1URL.getPath(), TEST1_SCHEMA, new Configuration());
		int[] projectionMask = {11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0};
		rowOrcInputFormat.setFieldMapping(projectionMask);
		rowOrcInputFormat.openInputFormat();
		FileInputSplit[] inputSplits = rowOrcInputFormat.createInputSplits(1);

		Assert.assertEquals(inputSplits.length, 1);

		Row row = null;
		int count = 0;
		for (FileInputSplit split : inputSplits) {
			rowOrcInputFormat.open(split);
			while (!rowOrcInputFormat.reachedEnd()) {
				row = rowOrcInputFormat.nextRecord(row);
				Assert.assertEquals(row.toString(), TEST1_PROJECTED_DATA[count++]);
			}
		}

	}

	@Test
	public void testTimeStampAndDate() throws IOException{

		URL expectedDataURL = getClass().getClassLoader().getResource("TestOrcFile.testDate1900.dat");
		assert(expectedDataURL != null);
		List<String> expectedTimeStampAndDate = Files.readAllLines(Paths.get(expectedDataURL.getPath()));

		URL testInputURL = getClass().getClassLoader().getResource("TestOrcFile.testDate1900.orc");
		assert(testInputURL != null);
		String path = testInputURL.getPath();
		String schema = "struct<time:timestamp,date:date>";
		rowOrcInputFormat = new RowOrcInputFormat(path, schema, new Configuration());
		rowOrcInputFormat.openInputFormat();

		FileInputSplit[] inputSplits = rowOrcInputFormat.createInputSplits(1);

		Assert.assertEquals(inputSplits.length, 1);

		List<Object> actualTimeStampAndDate = new ArrayList<>();

		Row row = null;
		int count = 0;
		for (FileInputSplit split : inputSplits) {
			rowOrcInputFormat.open(split);
			while (!rowOrcInputFormat.reachedEnd()) {
				row = rowOrcInputFormat.nextRecord(row);
				count++;
				if (count <= 10000) {
					actualTimeStampAndDate.add(row.getField(0) + "," + row.getField(1));
				}

			}
		}
		Assert.assertEquals(count, 70000);
		Assert.assertEquals(expectedTimeStampAndDate.size(), actualTimeStampAndDate.size());
		Assert.assertEquals(expectedTimeStampAndDate.toString(), actualTimeStampAndDate.toString());

	}

	@Test
	public void testDecimal() throws IOException{

		URL expectedDataURL = getClass().getClassLoader().getResource("decimal.dat");
		List<String> expectedDecimal = Files.readAllLines(Paths.get(expectedDataURL.getPath()));

		URL testInputURL = getClass().getClassLoader().getResource("decimal.orc");
		assert(testInputURL != null);
		String path = testInputURL.getPath();
		String schema = "struct<_col0:decimal(10,5)>";
		rowOrcInputFormat = new RowOrcInputFormat(path, schema, new Configuration());
		rowOrcInputFormat.openInputFormat();

		FileInputSplit[] inputSplits = rowOrcInputFormat.createInputSplits(1);

		Assert.assertEquals(inputSplits.length, 1);

		List<Object> actualDecimal = new ArrayList<>();

		Row row = null;
		for (FileInputSplit split : inputSplits) {
			rowOrcInputFormat.open(split);
			while (!rowOrcInputFormat.reachedEnd()) {
				row = rowOrcInputFormat.nextRecord(row);
				actualDecimal.add(row.getField(0));
			}
		}

		Assert.assertEquals(expectedDecimal.size(), actualDecimal.size());
		Assert.assertEquals(expectedDecimal.toString(), actualDecimal.toString());

	}

	@Test
	public void testEmptyFile() throws IOException{

		URL testInputURL = getClass().getClassLoader().getResource("TestOrcFile.emptyFile.orc");
		assert(testInputURL != null);
		String path = testInputURL.getPath();

		rowOrcInputFormat = new RowOrcInputFormat(path, TEST1_SCHEMA, new Configuration());
		rowOrcInputFormat.openInputFormat();

		FileInputSplit[] inputSplits = rowOrcInputFormat.createInputSplits(1);

		Assert.assertEquals(inputSplits.length, 1);

		Row row = new Row(1);
		int count = 0;
		for (FileInputSplit split : inputSplits) {
			rowOrcInputFormat.open(split);
			while (!rowOrcInputFormat.reachedEnd()) {
				row = rowOrcInputFormat.nextRecord(row);
				count++;
			}
		}

		Assert.assertEquals(count, 0);
	}

	@Test
	public void testLargeFile() throws IOException{

		URL testInputURL = getClass().getClassLoader().getResource("demo-11-none.orc");
		assert(testInputURL != null);
		String path = testInputURL.getPath();
		String schema = "struct<_col0:int,_col1:string,_col2:string,_col3:string,_col4:int," +
			"_col5:string,_col6:int,_col7:int,_col8:int>";

		rowOrcInputFormat = new RowOrcInputFormat(path, schema, new Configuration());
		rowOrcInputFormat.openInputFormat();

		FileInputSplit[] inputSplits = rowOrcInputFormat.createInputSplits(1);

		Assert.assertEquals(inputSplits.length, 1);

		Row row = new Row(1);
		int count = 0;
		for (FileInputSplit split : inputSplits) {
			rowOrcInputFormat.open(split);
			while (!rowOrcInputFormat.reachedEnd()) {
				row = rowOrcInputFormat.nextRecord(row);
				count++;
			}
		}

		Assert.assertEquals(count, 1920800);
	}

	@Test
	public void testProducedType() throws IOException{

		assert(test1URL != null);
		rowOrcInputFormat = new RowOrcInputFormat(test1URL.getPath(), TEST1_SCHEMA, new Configuration());
		rowOrcInputFormat.openInputFormat();
		FileInputSplit[] inputSplits = rowOrcInputFormat.createInputSplits(1);

		Assert.assertEquals(inputSplits.length, 1);

		rowOrcInputFormat.open(inputSplits[0]);

		TypeInformation<Row> type = rowOrcInputFormat.getProducedType();
		Assert.assertEquals(type.toString(), "Row(boolean1: Boolean, byte1: Byte, short1: Short, int1: Integer," +
			" long1: Long, float1: Float, double1: Double, bytes1: byte[], string1: String," +
			" middle: Row(list: ObjectArrayTypeInfo<Row(int1: Integer, string1: String)>)," +
			" list: ObjectArrayTypeInfo<Row(int1: Integer, string1: String)>," +
			" map: Map<String, Row(int1: Integer, string1: String)>)");

	}

	@Test
	public void testProducedTypeWithProjection() throws IOException{

		assert(test1URL != null);
		rowOrcInputFormat = new RowOrcInputFormat(test1URL.getPath(), TEST1_SCHEMA, new Configuration());
		int[] projectionMask = {9, 10, 11};
		rowOrcInputFormat.setFieldMapping(projectionMask);
		rowOrcInputFormat.openInputFormat();
		FileInputSplit[] inputSplits = rowOrcInputFormat.createInputSplits(1);

		Assert.assertEquals(inputSplits.length, 1);

		rowOrcInputFormat.open(inputSplits[0]);

		TypeInformation<Row> type = rowOrcInputFormat.getProducedType();
		Assert.assertEquals(type.toString(), "Row(middle: Row(list: ObjectArrayTypeInfo<Row(int1: Integer, string1: String)>)," +
			" list: ObjectArrayTypeInfo<Row(int1: Integer, string1: String)>," +
			" map: Map<String, Row(int1: Integer, string1: String)>)");

	}

	@Test
	public void testLongList() throws Exception {

		URL testInputURL = getClass().getClassLoader().getResource("TestOrcFile.listlong.orc");
		assert(testInputURL != null);
		String path = testInputURL.getPath();
		String schema = "struct<mylist1:array<bigint>>";

		rowOrcInputFormat = new RowOrcInputFormat(path, schema, new Configuration());

		rowOrcInputFormat.openInputFormat();
		FileInputSplit[] inputSplits = rowOrcInputFormat.createInputSplits(1);

		Assert.assertEquals(inputSplits.length, 1);

		Row row = null;
		long count = 0;
		for (FileInputSplit split : inputSplits) {
			rowOrcInputFormat.open(split);
			while (!rowOrcInputFormat.reachedEnd()) {
				row = rowOrcInputFormat.nextRecord(row);
				Assert.assertEquals(row.getArity(), 1);
				Object object = row.getField(0);
				long[] l = (long[]) object;

				Assert.assertEquals(l.length, 2);
				if (count < 50) {
					Assert.assertArrayEquals(l, new long[]{count, count + 1});
				}
				else {
					Assert.assertArrayEquals(l, new long[]{0L, 0L});
				}
				count = count + 2;
			}
		}
		Assert.assertEquals(count, 100);
	}

	@Test
	public void testStringList() throws Exception {

		URL testInputURL = getClass().getClassLoader().getResource("TestOrcFile.liststring.orc");
		assert(testInputURL != null);
		String path = testInputURL.getPath();
		String schema = "struct<mylist1:array<string>>";

		rowOrcInputFormat = new RowOrcInputFormat(path, schema, new Configuration());

		rowOrcInputFormat.openInputFormat();
		FileInputSplit[] inputSplits = rowOrcInputFormat.createInputSplits(1);

		Assert.assertEquals(inputSplits.length, 1);

		Row row = null;
		long count = 0;
		for (FileInputSplit split : inputSplits) {
			rowOrcInputFormat.open(split);
			while (!rowOrcInputFormat.reachedEnd()) {
				row = rowOrcInputFormat.nextRecord(row);
				Assert.assertEquals(row.getArity(), 1);
				Object object = row.getField(0);
				String[] l = (String[]) object;

				Assert.assertEquals(l.length, 2);
				Assert.assertArrayEquals(l, new String[]{"hello" + count, "hello" + (count + 1) });
				count = count + 2;
			}
		}
		Assert.assertEquals(count, 200);
	}

	@Test
	public void testListOfListOfStructOfLong() throws Exception {
		URL testInputURL = getClass().getClassLoader().getResource("TestOrcFile.listliststructlong.orc");
		assert(testInputURL != null);
		String path = testInputURL.getPath();
		String schema = "struct<mylist1:array<array<struct<mylong1:bigint>>>>";

		rowOrcInputFormat = new RowOrcInputFormat(path, schema, new Configuration());

		rowOrcInputFormat.openInputFormat();
		FileInputSplit[] inputSplits = rowOrcInputFormat.createInputSplits(1);

		Assert.assertEquals(inputSplits.length, 1);

		Row row = null;
		long count = 0;
		for (FileInputSplit split : inputSplits) {
			rowOrcInputFormat.open(split);
			while (!rowOrcInputFormat.reachedEnd()) {

				row = rowOrcInputFormat.nextRecord(row);
				Assert.assertEquals(row.getArity(), 1);

				Object[] objects = (Object[]) row.getField(0);
				Assert.assertEquals(objects.length, 1);

				Object[] objects1 = (Object[]) objects[0];
				Assert.assertEquals(objects1.length, 1);

				Row[] nestedRows = Arrays.copyOf(objects1, objects1.length, Row[].class);
				Assert.assertEquals(nestedRows.length, 1);

				Assert.assertEquals(nestedRows[0].getArity(), 1);

				Assert.assertEquals(nestedRows[0].getField(0), count);

				count++;
			}
		}
		Assert.assertEquals(count, 100);
	}

	@Test
	public void testSplit() throws IOException{

		URL testInputURL = getClass().getClassLoader().getResource("demo-11-none.orc");
		assert(testInputURL != null);
		String path = testInputURL.getPath();
		String schema = "struct<_col0:int,_col1:string,_col2:string,_col3:string,_col4:int," +
			"_col5:string,_col6:int,_col7:int,_col8:int>";

		rowOrcInputFormat = new RowOrcInputFormat(path, schema, new Configuration());
		rowOrcInputFormat.openInputFormat();

		FileInputSplit[] inputSplits = rowOrcInputFormat.createInputSplits(10);

		Assert.assertEquals(inputSplits.length, 10);

		Row row = null;
		int countTotalRecords = 0;
		for (FileInputSplit split : inputSplits) {
			rowOrcInputFormat.open(split);
			int countSplitRecords = 0;
			while (!rowOrcInputFormat.reachedEnd()) {
				row = rowOrcInputFormat.nextRecord(row);
				countSplitRecords++;
			}
			Assert.assertNotEquals(countSplitRecords, 1920800);
			countTotalRecords += countSplitRecords;
		}

		Assert.assertEquals(countTotalRecords, 1920800);
	}

}
