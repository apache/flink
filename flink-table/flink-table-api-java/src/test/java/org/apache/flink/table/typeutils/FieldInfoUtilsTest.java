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
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.PROCTIME;

/**
 * Test suite for {@link FieldInfoUtils}.
 */
@RunWith(Enclosed.class)
public class FieldInfoUtilsTest {

	/**
	 * Test for ByNameMode.
	 */
	@RunWith(Parameterized.class)
	public static final class TestByNameMode {

		@Parameterized.Parameters(name = "{0}")
		public static Collection<TypeInformation> parameters() throws Exception {
			return Arrays.asList(
				new RowTypeInfo(
					new TypeInformation[]{Types.INT, Types.LONG, Types.SQL_TIMESTAMP},
					new String[]{"f0", "f1", "f2"}),
				new PojoTypeInfo(MyPojo.class, Arrays.asList(
					new PojoField(MyPojo.class.getDeclaredField("f0"), Types.INT),
					new PojoField(MyPojo.class.getDeclaredField("f1"), Types.LONG),
					new PojoField(MyPojo.class.getDeclaredField("f2"), Types.SQL_TIMESTAMP))));
		}

		@Parameterized.Parameter
		public TypeInformation typeInfo;

		@Test
		public void testByNameModeReorder() {
			FieldInfoUtils.TypeInfoSchema schema = FieldInfoUtils.getFieldsInfo(
				typeInfo,
				new Expression[]{
					new UnresolvedReferenceExpression("f2"),
					new UnresolvedReferenceExpression("f1"),
					new UnresolvedReferenceExpression("f0")});

			Assert.assertArrayEquals(new String[]{"f2", "f1", "f0"}, schema.getFieldNames());
		}

		@Test
		public void testByNameModeReorderAndRename() {
			FieldInfoUtils.TypeInfoSchema schema = FieldInfoUtils.getFieldsInfo(
				typeInfo,
				new Expression[]{
					new UnresolvedCallExpression(
						BuiltInFunctionDefinitions.AS,
						Arrays.asList(new UnresolvedReferenceExpression("f1"), new ValueLiteralExpression("aa"))),
					new UnresolvedCallExpression(
						BuiltInFunctionDefinitions.AS,
						Arrays.asList(new UnresolvedReferenceExpression("f0"), new ValueLiteralExpression("bb"))),
					new UnresolvedCallExpression(
						BuiltInFunctionDefinitions.AS,
						Arrays.asList(new UnresolvedReferenceExpression("f2"), new ValueLiteralExpression("cc")))
				});

			Assert.assertArrayEquals(new String[]{"aa", "bb", "cc"}, schema.getFieldNames());
		}

		/**
		 * Test Pojo class.
		 */
		public static class MyPojo {
			public int f0;
			public long f1;
			public Timestamp f2;

			public MyPojo() {
			}
		}
	}

	/**
	 * Test for ByPositionMode.
	 */
	public static final class TestByPositionMode {
		private static final RowTypeInfo rowTypeInfo = new RowTypeInfo(
			new TypeInformation[]{Types.INT, Types.LONG, Types.SQL_TIMESTAMP},
			new String[]{"f0", "f1", "f2"});

		@Test
		public void testByPositionMode() {
			FieldInfoUtils.TypeInfoSchema schema = FieldInfoUtils.getFieldsInfo(
				rowTypeInfo,
				new Expression[]{
					new UnresolvedReferenceExpression("aa"),
					new UnresolvedReferenceExpression("bb"),
					new UnresolvedReferenceExpression("cc")});

			Assert.assertArrayEquals(new String[]{"aa", "bb", "cc"}, schema.getFieldNames());
		}

		@Test
		public void testByPositionModeProcTime() {
			FieldInfoUtils.TypeInfoSchema schema = FieldInfoUtils.getFieldsInfo(
				rowTypeInfo,
				new Expression[]{
					new UnresolvedReferenceExpression("aa"),
					new UnresolvedReferenceExpression("bb"),
					new UnresolvedReferenceExpression("cc"),
					new UnresolvedCallExpression(
						BuiltInFunctionDefinitions.AS,
						Arrays.asList(
							new UnresolvedCallExpression(
								PROCTIME,
								Arrays.asList(new UnresolvedReferenceExpression("cc"))),
							new ValueLiteralExpression("proctime")))});

			Assert.assertArrayEquals(new String[]{"aa", "bb", "cc", "proctime"}, schema.getFieldNames());
		}
	}
}
