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

package org.apache.flink.table.typeutils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.DataType;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Test suite for {@link FieldInfoUtils}.
 */
public class FieldInfoUtilsTest {

	private static final RowTypeInfo typeInfo = new RowTypeInfo(
		new TypeInformation[]{Types.INT, Types.LONG, Types.STRING},
		new String[]{"f0", "f1", "f2"});

	@Test
	public void testByPositionMode() {
		FieldInfoUtils.TypeInfoSchema schema = FieldInfoUtils.getFieldsInfo(
			typeInfo,
			new Expression[]{$("aa"), $("bb"), $("cc")});

		Assert.assertEquals("[aa, bb, cc]", Arrays.asList(schema.getFieldNames()).toString());
		Assert.assertArrayEquals(new DataType[]{DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING()}, schema.getFieldTypes());
	}

	@Test
	public void testByNameModeReorder() {
		FieldInfoUtils.TypeInfoSchema schema = FieldInfoUtils.getFieldsInfo(
			typeInfo,
			new Expression[]{$("f2"), $("f1"), $("f0")});

		Assert.assertEquals("[f2, f1, f0]", Arrays.asList(schema.getFieldNames()).toString());
		Assert.assertArrayEquals(new DataType[]{DataTypes.STRING(), DataTypes.BIGINT(), DataTypes.INT()}, schema.getFieldTypes());
	}

	@Test
	public void testByNameModeReorderAndRename() {
		FieldInfoUtils.TypeInfoSchema schema = FieldInfoUtils.getFieldsInfo(
			typeInfo,
			new Expression[]{$("f1").as("aa"), $("f0").as("bb"), $("f2").as("cc")});

		Assert.assertEquals("[aa, bb, cc]", Arrays.asList(schema.getFieldNames()).toString());
		Assert.assertArrayEquals(new DataType[]{DataTypes.BIGINT(), DataTypes.INT(), DataTypes.STRING()}, schema.getFieldTypes());
	}
}
