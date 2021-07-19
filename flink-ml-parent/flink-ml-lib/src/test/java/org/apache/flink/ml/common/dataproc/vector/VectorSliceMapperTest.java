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
import org.apache.flink.ml.params.dataproc.vector.VectorSliceParams;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

/**
 * Unit test for VectorSliceMapper.
 */
public class VectorSliceMapperTest {
	@Test
	public void test1() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"vec"}, new TypeInformation<?>[]{Types.STRING});

		Params params = new Params()
			.set(VectorSliceParams.SELECTED_COL, "vec")
			.set(VectorSliceParams.INDICES, new int[]{0, 1});

		VectorSliceMapper mapper = new VectorSliceMapper(schema, params);

		assertEquals(mapper.map(Row.of(new DenseVector(new double[]{3.0, 4.0, 3.0}))).getField(0),
			new DenseVector(new double[]{3.0, 4.0}));
		assertEquals(mapper.getOutputSchema(),
			new TableSchema(new String[]{"vec"}, new TypeInformation<?>[]{VectorTypes.VECTOR}));
	}

	@Test
	public void test2() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"vec"}, new TypeInformation<?>[]{Types.STRING});

		Params params = new Params()
			.set(VectorSliceParams.SELECTED_COL, "vec")
			.set(VectorSliceParams.OUTPUT_COL, "res")
			.set(VectorSliceParams.INDICES, new int[]{0, 1});

		VectorSliceMapper mapper = new VectorSliceMapper(schema, params);

		assertEquals(mapper.map(Row.of(new DenseVector(new double[]{3.0, 4.0, 3.0}))).getField(1),
			new DenseVector(new double[]{3.0, 4.0}));
		assertEquals(
			mapper.getOutputSchema(),
			new TableSchema(new String[]{"vec", "res"},
				new TypeInformation<?>[]{Types.STRING, VectorTypes.VECTOR})
		);
	}

	@Test
	public void test3() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"vec"}, new TypeInformation<?>[]{Types.STRING});

		Params params = new Params()
			.set(VectorSliceParams.SELECTED_COL, "vec")
			.set(VectorSliceParams.OUTPUT_COL, "res")
			.set(VectorSliceParams.RESERVED_COLS, new String[]{})
			.set(VectorSliceParams.INDICES, new int[]{0, 2, 4});

		VectorSliceMapper mapper = new VectorSliceMapper(schema, params);

		assertEquals(mapper.map(Row.of(new SparseVector(5, new int[]{0, 2, 4}, new double[]{3.0, 4.0, 3.0})))
			.getField(0), new SparseVector(3, new int[]{0, 1, 2}, new double[]{3.0, 4.0, 3.0}));
		assertEquals(
			mapper.getOutputSchema(),
			new TableSchema(new String[]{"res"}, new TypeInformation<?>[]{VectorTypes.VECTOR})
		);
	}
}

