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

package org.apache.flink.ml.common.ml.feature;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.common.linalg.VectorUtil;
import org.apache.flink.ml.common.utils.VectorTypes;
import org.apache.flink.ml.params.ml.feature.BinarizerParams;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for BinarizerMapper.
 */
public class BinarizerMapperTest {
	@Test
	public void test1() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"feature"}, new TypeInformation<?>[] {VectorTypes.VECTOR});

		Params params = new Params()
			.set(BinarizerParams.SELECTED_COL, "feature");

		BinarizerMapper mapper = new BinarizerMapper(schema, params);

		assertEquals(mapper.map(Row.of(VectorUtil.parse("0.1 0.6"))).getField(0), VectorUtil.parse("1.0 1.0"));
		assertEquals(mapper.map(Row.of(VectorUtil.parse("$20$4:0.2 6:1.0 7:0.05"))).getField(0),
			VectorUtil.parse("$20$4:1.0 6:1.0 7:1.0"));
		assertEquals(mapper.getOutputSchema(),
			new TableSchema(new String[] {"feature"}, new TypeInformation<?>[] {VectorTypes.VECTOR}));
	}

	@Test
	public void test2() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"feature"}, new TypeInformation<?>[] {VectorTypes.VECTOR});

		Params params = new Params()
			.set(BinarizerParams.SELECTED_COL, "feature")
			.set(BinarizerParams.THRESHOLD, 1.5);

		BinarizerMapper mapper = new BinarizerMapper(schema, params);

		assertEquals(mapper.map(Row.of(VectorUtil.parse("0.1 0.6"))).getField(0), VectorUtil.parse("$2$"));
		assertEquals(mapper.map(Row.of(VectorUtil.parse("2.1 2.6 4.1 0.6 3.2"))).getField(0), VectorUtil.parse("1.0 1.0 1.0 0.0 1.0"));
		assertEquals(mapper.map(Row.of(VectorUtil.parse("$20$4:0.2 6:1.0 7:0.05"))).getField(0), VectorUtil.parse("$20$"));

		assertEquals(mapper.getOutputSchema(),
			new TableSchema(new String[] {"feature"}, new TypeInformation<?>[] {VectorTypes.VECTOR}));
	}

	@Test
	public void test3() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"feature"}, new TypeInformation<?>[] {Types.DOUBLE});

		Params params = new Params()
			.set(BinarizerParams.SELECTED_COL, "feature");

		BinarizerMapper mapper = new BinarizerMapper(schema, params);

		assertEquals(mapper.map(Row.of(0.6)).getField(0), 1.0);
		assertEquals(mapper.getOutputSchema(), schema);
	}

	@Test
	public void test4() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"feature"}, new TypeInformation<?>[] {Types.DOUBLE});

		Params params = new Params()
			.set(BinarizerParams.SELECTED_COL, "feature")
			.set(BinarizerParams.OUTPUT_COL, "output")
			.set(BinarizerParams.THRESHOLD, 1.0);

		BinarizerMapper mapper = new BinarizerMapper(schema, params);

		assertEquals(mapper.map(Row.of(0.6)).getField(1), 0.0);
		assertEquals(mapper.getOutputSchema(),
			new TableSchema(new String[] {"feature", "output"},
				new TypeInformation<?>[] {Types.DOUBLE, Types.DOUBLE})
		);
	}

	@Test
	public void test5() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"feature"}, new TypeInformation<?>[] {Types.LONG});

		Params params = new Params()
			.set(BinarizerParams.SELECTED_COL, "feature");

		BinarizerMapper mapper = new BinarizerMapper(schema, params);

		assertEquals(mapper.map(Row.of(4L)).getField(0), 1L);
		assertEquals(mapper.getOutputSchema(), schema);
	}
}
