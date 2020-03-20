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

package org.apache.flink.streaming.connectors.elasticsearch;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableException;
import org.apache.flink.types.Row;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Suite tests for {@link IndexFormatter}.
 */
public class IndexFormatterTest {
	private String[] fieldNames;
	private TypeInformation[] fieldTypes;
	private List<Row> rows;

	@Before
	public void prepareData() {
		fieldNames = new String[]{"id", "item", "log_ts", "log_date", "order_timestamp", "note"};
		fieldTypes = new TypeInformation[] {
			Types.INT,
			Types.STRING,
			Types.LONG,
			Types.SQL_DATE,
			Types.SQL_TIMESTAMP,
			Types.STRING
		};
		rows = new ArrayList<>();
		rows.add(Row.of(
			1,
			"apple",
			Timestamp.valueOf("2020-03-18 12:12:14").getTime(),
			Date.valueOf("2020-03-18"),
			Timestamp.valueOf("2020-03-18 12:12:14"),
			"test1"));
		rows.add(Row.of(
			2,
			"peanut",
			Timestamp.valueOf("2020-03-19 12:12:14").getTime(),
			Date.valueOf("2020-03-19"),
			Timestamp.valueOf("2020-03-19 12:22:21"),
			"test2"));
	}

	@Test
	public void testDynamicIndexEnabled() {
		IndexFormatter indexFormatter = IndexFormatter.builder()
			.index("my-index-{log_ts|yyyy-MM-dd HH:MM:ss}")
			.fieldNames(fieldNames)
			.fieldTypes(fieldTypes)
			.build();
		Assert.assertTrue(indexFormatter.isDynamicIndexEnabled());
		IndexFormatter indexFormatter1 = IndexFormatter.builder()
			.index("my-index")
			.fieldNames(fieldNames)
			.fieldTypes(fieldTypes)
			.build();
		Assert.assertFalse(indexFormatter1.isDynamicIndexEnabled());
	}

	@Test
	public void testDynamicIndexFromTimestamp() {
		IndexFormatter indexFormatter = IndexFormatter.builder()
			.index("{order_timestamp|yyyy_MM_dd_HH-ss}_index")
			.fieldNames(fieldNames)
			.fieldTypes(fieldTypes)
			.build();
		Assert.assertEquals("2020_03_18_12-14_index", indexFormatter.getFormattedIndex(rows.get(0)));
		IndexFormatter indexFormatter1 = IndexFormatter.builder()
			.index("{order_timestamp|yyyy_MM_dd_HH_mm}_index")
			.fieldNames(fieldNames)
			.fieldTypes(fieldTypes)
			.build();
		Assert.assertEquals("2020_03_18_12_12_index", indexFormatter1.getFormattedIndex(rows.get(0)));
	}

	@Test
	public void testDynamicIndexFromLong() {
		IndexFormatter indexFormatter = IndexFormatter.builder()
			.index("my-index-{log_ts|yyyy-MM-dd}")
			.fieldNames(fieldNames)
			.fieldTypes(fieldTypes)
			.build();
		Assert.assertEquals("my-index-2020-03-18", indexFormatter.getFormattedIndex(rows.get(0)));
		Assert.assertEquals("my-index-2020-03-19", indexFormatter.getFormattedIndex(rows.get(1)));
	}

	@Test
	public void testDynamicIndexFromDate() {
		IndexFormatter indexFormatter = IndexFormatter.builder()
			.index("my-index-{log_date|yyyy/MM/dd/HH}")
			.fieldNames(fieldNames)
			.fieldTypes(fieldTypes)
			.build();
		Assert.assertEquals("my-index-2020/03/18/00", indexFormatter.getFormattedIndex(rows.get(0)));
		Assert.assertEquals("my-index-2020/03/19/00", indexFormatter.getFormattedIndex(rows.get(1)));

	}

	@Test
	public void testGeneralDynamicIndex() {
		IndexFormatter indexFormatter = IndexFormatter.builder()
			.index("index_{item}")
			.fieldNames(fieldNames)
			.fieldTypes(fieldTypes)
			.build();
		Assert.assertEquals("index_apple", indexFormatter.getFormattedIndex(rows.get(0)));
		Assert.assertEquals("index_peanut", indexFormatter.getFormattedIndex(rows.get(1)));
	}

	@Test
	public void testStaticIndex() {
		IndexFormatter indexFormatter = IndexFormatter.builder()
			.index("my-index")
			.fieldNames(fieldNames)
			.fieldTypes(fieldTypes)
			.build();
		Assert.assertFalse(indexFormatter.isDynamicIndexEnabled());
		Assert.assertEquals("my-index", indexFormatter.getFormattedIndex(rows.get(0)));
		Assert.assertEquals("my-index", indexFormatter.getFormattedIndex(rows.get(1)));
	}

	@Test(expected = TableException.class)
	public void testUnknownField() {
		IndexFormatter indexFormatter = IndexFormatter.builder()
			.index("my-index-{unknown_ts|yyyy-MM-dd}")
			.fieldNames(fieldNames)
			.fieldTypes(fieldTypes)
			.build();
		Assert.assertTrue(indexFormatter.isDynamicIndexEnabled());
		Assert.assertEquals("my-index", indexFormatter.getFormattedIndex(rows.get(0)));
	}

	@Test(expected = TableException.class)
	public void testUnSupportedType() {
		IndexFormatter indexFormatter = IndexFormatter.builder()
			.index("my-index-{id|yyyy-MM-dd}")
			.fieldNames(fieldNames)
			.fieldTypes(fieldTypes)
			.build();
		Assert.assertTrue(indexFormatter.isDynamicIndexEnabled());
		Assert.assertEquals("my-index", indexFormatter.getFormattedIndex(rows.get(0)));
	}

}
