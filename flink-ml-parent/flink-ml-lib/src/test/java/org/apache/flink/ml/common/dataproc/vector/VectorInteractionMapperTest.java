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
import org.apache.flink.ml.common.linalg.DenseVector;
import org.apache.flink.ml.common.linalg.SparseVector;
import org.apache.flink.ml.common.utils.VectorTypes;
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
			new TypeInformation<?>[]{Types.STRING, Types.STRING, VectorTypes.VECTOR});

		Params params = new Params()
			.set(VectorInteractionParams.SELECTED_COLS, new String[]{"c0", "c1"})
			.set(VectorInteractionParams.OUTPUT_COL, "out");

		VectorInteractionMapper mapper = new VectorInteractionMapper(schema, params);

		assertEquals(mapper.map(Row.of(new DenseVector(new double[]{3.0, 4.0}), new DenseVector(new double[]{3.0, 4.0})))
			.getField(2), new DenseVector(new double[]{9.0, 12.0, 12.0, 16.0}));
		assertEquals(mapper.getOutputSchema(), outSchema);
	}

	@Test
	public void test2() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"c0", "c1"},
			new TypeInformation<?>[]{Types.STRING, Types.STRING});

		TableSchema outSchema = new TableSchema(new String[]{"c0", "out"},
			new TypeInformation<?>[]{Types.STRING, VectorTypes.VECTOR});

		Params params = new Params()
			.set(VectorInteractionParams.SELECTED_COLS, new String[]{"c0", "c1"})
			.set(VectorInteractionParams.OUTPUT_COL, "out")
			.set(VectorInteractionParams.RESERVED_COLS, new String[]{"c0"});

		VectorInteractionMapper mapper = new VectorInteractionMapper(schema, params);

		assertEquals(mapper.map(Row.of(new DenseVector(new double[]{3.0, 4.0}),
			new DenseVector(new double[]{3.0, 4.0}))).getField(1),
			new DenseVector(new double[]{9.0, 12.0, 12.0, 16.0}));
		assertEquals(mapper.getOutputSchema(), outSchema);
	}

	@Test
	public void test3() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"c0", "c1"},
			new TypeInformation<?>[]{Types.STRING, Types.STRING});

		TableSchema outSchema = new TableSchema(new String[]{"c0", "out"},
			new TypeInformation<?>[]{Types.STRING, VectorTypes.VECTOR});

		Params params = new Params()
			.set(VectorInteractionParams.SELECTED_COLS, new String[]{"c0", "c1"})
			.set(VectorInteractionParams.OUTPUT_COL, "out")
			.set(VectorInteractionParams.RESERVED_COLS, new String[]{"c0"});

		VectorInteractionMapper mapper = new VectorInteractionMapper(schema, params);

		assertEquals(mapper.map(Row.of(new SparseVector(10, new int[]{0, 9}, new double[]{1.0, 4.0}),
			new SparseVector(10, new int[]{0, 9}, new double[]{1.0, 4.0}))).getField(1),
			new SparseVector(100, new int[]{0, 9, 90, 99}, new double[]{1.0, 4.0, 4.0, 16.0}));
		assertEquals(mapper.getOutputSchema(), outSchema);
	}
}
