/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.model;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Unit test cases for {@link ModelConverterUtils}.
 */
public class ModelConverterUtilsTest {

	class MockCollector implements Collector<Row> {
		List<Row> buffer = new ArrayList<>();

		@Override
		public void collect(Row record) {
			buffer.add(record);
		}

		@Override
		public void close() {
			buffer.clear();
		}
	}

	private static final ParamInfo<String> SOME_PARAM = ParamInfoFactory
		.createParamInfo("key", String.class)
		.build();

	@Test
	public void testAppendMetaRow() throws Exception {
		Params meta = new Params().set(SOME_PARAM, "value");
		MockCollector collector = new MockCollector();
		ModelConverterUtils.appendMetaRow(meta, collector, 2);
		Row row = collector.buffer.get(0);
		Assert.assertEquals(row.getArity(), 2);
		Assert.assertEquals(row.getField(0), 0L);
		Assert.assertEquals(row.getField(1), meta.toJson());
	}

	@Test
	public void testAppendDataRows() throws Exception {
		List<String> stringList = new ArrayList<>();
		stringList.add("apple");
		stringList.add("banana");
		MockCollector collector = new MockCollector();
		ModelConverterUtils.appendDataRows(stringList, collector, 2);
		Row row1 = collector.buffer.get(0);
		Assert.assertEquals(row1.getArity(), 2);
		Assert.assertEquals(row1.getField(0), ModelConverterUtils.MAX_NUM_SLICES);
		Assert.assertEquals(row1.getField(1), "apple");
		Row row2 = collector.buffer.get(1);
		Assert.assertEquals(row2.getArity(), 2);
		Assert.assertEquals(row2.getField(0), 2 * ModelConverterUtils.MAX_NUM_SLICES);
		Assert.assertEquals(row2.getField(1), "banana");
	}

	@Test
	public void testAppendAuxiliaryData() throws Exception {
		List<String> stringList = new ArrayList<>();
		stringList.add("apple");
		stringList.add("banana");
		MockCollector collector = new MockCollector();
		ModelConverterUtils.appendAuxiliaryData(stringList, collector, 3);
		Row row1 = collector.buffer.get(0);
		Assert.assertEquals(row1.getArity(), 3);
		Assert.assertEquals(row1.getField(0), Integer.MAX_VALUE * ModelConverterUtils.MAX_NUM_SLICES);
		Assert.assertEquals(row1.getField(2), "apple");
		Row row2 = collector.buffer.get(1);
		Assert.assertEquals(row2.getArity(), 3);
		Assert.assertEquals(row2.getField(0), Integer.MAX_VALUE * ModelConverterUtils.MAX_NUM_SLICES + 1L);
		Assert.assertEquals(row2.getField(2), "banana");
	}

	@Test
	public void testExtractModelMetaAndData() throws Exception {
		Params meta = new Params().set(SOME_PARAM, "value");
		List<String> stringList = new ArrayList<>();
		stringList.add("apple");
		stringList.add("banana");
		MockCollector collector = new MockCollector();
		ModelConverterUtils.appendMetaRow(meta, collector, 2);
		ModelConverterUtils.appendDataRows(stringList, collector, 2);

		Tuple2<Params, Iterable<String>> metaAndData = ModelConverterUtils.extractModelMetaAndData(collector.buffer);
		Assert.assertEquals(metaAndData.f0.toJson(), meta.toJson());
		Iterator<String> stringIterator = metaAndData.f1.iterator();
		Assert.assertTrue(stringIterator.hasNext());
		Assert.assertEquals(stringIterator.next(), "apple");
		Assert.assertTrue(stringIterator.hasNext());
		Assert.assertEquals(stringIterator.next(), "banana");
	}

	@Test
	public void testExtractAuxiliaryData() throws Exception {
		Params meta = new Params().set(SOME_PARAM, "value");
		List<String> stringList = new ArrayList<>();
		stringList.add("apple");
		stringList.add("banana");
		MockCollector collector = new MockCollector();
		ModelConverterUtils.appendMetaRow(meta, collector, 3);
		ModelConverterUtils.appendDataRows(stringList, collector, 3);
		ModelConverterUtils.appendAuxiliaryData(stringList, collector, 3);

		Iterable<Object> labels = ModelConverterUtils.extractAuxiliaryData(collector.buffer, true);
		Iterator<Object> labelIterator = labels.iterator();
		Assert.assertTrue(labelIterator.hasNext());
		Assert.assertEquals(labelIterator.next(), "apple");
		Assert.assertTrue(labelIterator.hasNext());
		Assert.assertEquals(labelIterator.next(), "banana");
	}

}
