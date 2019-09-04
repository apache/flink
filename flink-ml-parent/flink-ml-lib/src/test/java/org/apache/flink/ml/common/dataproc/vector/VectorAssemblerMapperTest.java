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
import org.apache.flink.ml.params.dataproc.vector.VectorAssemblerParams;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for VectorAssemblerMapper.
 */
public class VectorAssemblerMapperTest {
	@Test
	public void test1() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"c0", "c1", "c2"},
			new TypeInformation<?>[]{Types.STRING, Types.DOUBLE, Types.STRING});

		TableSchema outSchema = new TableSchema(new String[]{"c0", "c1", "c2", "out"},
			new TypeInformation<?>[]{Types.STRING, Types.DOUBLE, Types.STRING, VectorTypes.VECTOR});

		Params params = new Params()
			.set(VectorAssemblerParams.SELECTED_COLS, new String[]{"c0", "c1", "c2"})
			.set(VectorAssemblerParams.OUTPUT_COL, "out");

		VectorAssemblerMapper mapper = new VectorAssemblerMapper(schema, params);
		/* join the DenseVector, the number and the SparseVector together. the forth field shows the result */
		assertEquals(mapper.map(Row.of(new DenseVector(new double[]{3.0, 4.0}), 3.0,
			new SparseVector(3, new int[]{0, 2}, new double[]{1.0, 4.0}))).getField(3),
			new DenseVector(new double[]{3.0, 4.0, 3.0, 1.0, 0.0, 4.0}));
		assertEquals(mapper.getOutputSchema(), outSchema);
	}

	@Test
	public void test2() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"c0", "c1", "c2"},
			new TypeInformation<?>[]{Types.STRING, Types.DOUBLE, Types.STRING});

		TableSchema outSchema = new TableSchema(new String[]{"c0", "out"},
			new TypeInformation<?>[]{Types.STRING, VectorTypes.VECTOR});

		Params params = new Params()
			.set(VectorAssemblerParams.SELECTED_COLS, new String[]{"c0", "c1", "c2"})
			.set(VectorAssemblerParams.OUTPUT_COL, "out")
			.set(VectorAssemblerParams.RESERVED_COLS, new String[]{"c0"});

		VectorAssemblerMapper mapper = new VectorAssemblerMapper(schema, params);
		/* only reverse one column. */
		assertEquals(mapper.map(Row.of(new DenseVector(new double[]{3.0, 4.0}), 3.0,
			new SparseVector(11, new int[]{0, 10}, new double[]{1.0, 4.0}))).getField(1),
			new SparseVector(14, new int[]{0, 1, 2, 3, 13}, new double[]{3.0, 4.0, 3.0, 1.0, 4.0}));
		assertEquals(mapper.getOutputSchema(), outSchema);
	}

	@Test
	public void test3() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"c0", "c1", "c2"},
			new TypeInformation<?>[]{Types.STRING, Types.DOUBLE, Types.STRING});

		TableSchema outSchema = new TableSchema(new String[]{"c0", "out"},
			new TypeInformation<?>[]{Types.STRING, VectorTypes.VECTOR});

		Params params = new Params()
			.set(VectorAssemblerParams.SELECTED_COLS, new String[]{"c0", "c1", "c2"})
			.set(VectorAssemblerParams.OUTPUT_COL, "out")
			.set(VectorAssemblerParams.HANDLE_INVALID, "skip")
			.set(VectorAssemblerParams.RESERVED_COLS, new String[]{"c0"});

		VectorAssemblerMapper mapper = new VectorAssemblerMapper(schema, params);
		/* skip the invalid data. */
		assertEquals(mapper.map(Row.of(new DenseVector(new double[]{3.0, 4.0}), null,
			new SparseVector(11, new int[]{0, 10}, new double[]{1.0, 4.0}))).getField(1),
			new SparseVector(13, new int[]{0, 1, 2, 12}, new double[]{3.0, 4.0, 1.0, 4.0}));
		assertEquals(mapper.getOutputSchema(), outSchema);
	}

	@Test
	public void test4() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"c0", "c1", "c2"},
			new TypeInformation<?>[]{Types.STRING, Types.DOUBLE, Types.STRING});

		TableSchema outSchema = new TableSchema(new String[]{"c0", "out"},
			new TypeInformation<?>[]{Types.STRING, VectorTypes.VECTOR});

		Params params = new Params()
			.set(VectorAssemblerParams.SELECTED_COLS, new String[]{"c0", "c1", "c2"})
			.set(VectorAssemblerParams.OUTPUT_COL, "out")
			.set(VectorAssemblerParams.HANDLE_INVALID, "keep")
			.set(VectorAssemblerParams.RESERVED_COLS, new String[]{"c0"});

		VectorAssemblerMapper mapper = new VectorAssemblerMapper(schema, params);

		assertEquals(mapper.map(Row.of(new DenseVector(new double[]{3.0, 4.0}), null,
			new SparseVector(11, new int[]{0, 10}, new double[]{1.0, 4.0}))).getField(1),
			new SparseVector(14, new int[]{0, 1, 2, 3, 13}, new double[]{3.0, 4.0, Double.NaN, 1.0, 4.0}));
		assertEquals(mapper.getOutputSchema(), outSchema);
	}

}
