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

package org.apache.flink.ml.common.dataproc.vector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.params.dataproc.vector.VectorInteractionParams;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for VectorInteractionMapper.
 */
public class VectorInteractionMapperTest {
	@Test
	public void test1() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"c0", "c1"},
			new TypeInformation<?>[]{Types.STRING, Types.STRING});

		TableSchema outSchema = new TableSchema(new String[]{"c0", "c1", "out"},
			new TypeInformation<?>[]{Types.STRING, Types.STRING, Types.STRING});

		Params params = new Params()
			.set(VectorInteractionParams.SELECTED_COLS, new String[]{"c0", "c1"})
			.set(VectorInteractionParams.OUTPUT_COL, "out");

		VectorInteractionMapper mapper = new VectorInteractionMapper(schema, params);

		assertEquals(mapper.map(Row.of("3.0 4.0", "3.0 4.0")).getField(2).toString(), "9.0 12.0 12.0 16.0");
		assertEquals(mapper.getOutputSchema(), outSchema);
	}

	@Test
	public void test2() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"c0", "c1"},
			new TypeInformation<?>[]{Types.STRING, Types.STRING});

		TableSchema outSchema = new TableSchema(new String[]{"c0", "out"},
			new TypeInformation<?>[]{Types.STRING, Types.STRING});

		Params params = new Params()
			.set(VectorInteractionParams.SELECTED_COLS, new String[]{"c0", "c1"})
			.set(VectorInteractionParams.OUTPUT_COL, "out")
			.set(VectorInteractionParams.RESERVED_COLS, new String[]{"c0"});

		VectorInteractionMapper mapper = new VectorInteractionMapper(schema, params);

		assertEquals(mapper.map(Row.of("3.0 4.0", "3.0 4.0")).getField(1).toString(), "9.0 12.0 12.0 16.0");
		assertEquals(mapper.getOutputSchema(), outSchema);
	}

	@Test
	public void test3() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"c0", "c1"},
			new TypeInformation<?>[]{Types.STRING, Types.STRING});

		TableSchema outSchema = new TableSchema(new String[]{"c0", "out"},
			new TypeInformation<?>[]{Types.STRING, Types.STRING});

		Params params = new Params()
			.set(VectorInteractionParams.SELECTED_COLS, new String[]{"c0", "c1"})
			.set(VectorInteractionParams.OUTPUT_COL, "out")
			.set(VectorInteractionParams.RESERVED_COLS, new String[]{"c0"});

		VectorInteractionMapper mapper = new VectorInteractionMapper(schema, params);

		assertEquals(mapper.map(Row.of("$10$0:1.0 9:4.0", "$10$0:1.0 9:4.0")).getField(1).toString(),
			"$100$0:1.0 9:4.0 90:4.0 99:16.0");
		assertEquals(mapper.getOutputSchema(), outSchema);
	}
}
