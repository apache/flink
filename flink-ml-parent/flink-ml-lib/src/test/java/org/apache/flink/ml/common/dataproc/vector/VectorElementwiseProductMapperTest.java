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
import org.apache.flink.ml.params.dataproc.vector.VectorElementwiseProductParams;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for VectorElementwiseProductMapper.
 */
public class VectorElementwiseProductMapperTest {
	@Test
	public void test1() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"vec"}, new TypeInformation<?>[]{Types.STRING});

		Params params = new Params()
			.set(VectorElementwiseProductParams.SELECTED_COL, "vec")
			.set(VectorElementwiseProductParams.SCALING_VECTOR, "3.0 4.5");

		VectorElementwiseProductMapper mapper = new VectorElementwiseProductMapper(schema, params);

		assertEquals(mapper.map(Row.of(new DenseVector(new double[]{3.0, 4.0}))).getField(0),
			new DenseVector(new double[]{9.0, 18.0}));
		assertEquals(mapper.getOutputSchema(),
			new TableSchema(new String[]{"vec"}, new TypeInformation<?>[]{VectorTypes.VECTOR}));
	}

	@Test
	public void test2() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"vec"}, new TypeInformation<?>[]{Types.STRING});

		Params params = new Params()
			.set(VectorElementwiseProductParams.SELECTED_COL, "vec")
			.set(VectorElementwiseProductParams.OUTPUT_COL, "res")
			.set(VectorElementwiseProductParams.SCALING_VECTOR, "3.0 4.5");

		VectorElementwiseProductMapper mapper = new VectorElementwiseProductMapper(schema, params);

		assertEquals(mapper.map(Row.of(new DenseVector(new double[]{3.0, 4.0}))).getField(1),
			new DenseVector(new double[]{9.0, 18.0}));
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
			.set(VectorElementwiseProductParams.SELECTED_COL, "vec")
			.set(VectorElementwiseProductParams.OUTPUT_COL, "res")
			.set(VectorElementwiseProductParams.RESERVED_COLS, new String[]{})
			.set(VectorElementwiseProductParams.SCALING_VECTOR, "3.0 4.5");

		VectorElementwiseProductMapper mapper = new VectorElementwiseProductMapper(schema, params);

		assertEquals(mapper.map(Row.of(new DenseVector(new double[]{3.0, 4.0}))).getField(0),
			new DenseVector(new double[]{9.0, 18.0}));
		assertEquals(
			mapper.getOutputSchema(),
			new TableSchema(new String[]{"res"}, new TypeInformation<?>[]{VectorTypes.VECTOR})
		);
	}

	@Test
	public void test4() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"vec"}, new TypeInformation<?>[]{Types.STRING});

		Params params = new Params()
			.set(VectorElementwiseProductParams.SELECTED_COL, "vec")
			.set(VectorElementwiseProductParams.OUTPUT_COL, "res")
			.set(VectorElementwiseProductParams.SCALING_VECTOR, "$10$1:3.0 2:10.0 9:4.5");

		VectorElementwiseProductMapper mapper = new VectorElementwiseProductMapper(schema, params);

		assertEquals(mapper.map(Row.of(new SparseVector(10, new int[]{1, 5, 9}, new double[]{2.0, 4.0, 3.0})))
			.getField(1), new SparseVector(10, new int[]{1, 5, 9}, new double[]{6.0, 0.0, 13.5}));
		assertEquals(
			mapper.getOutputSchema(),
			new TableSchema(new String[]{"vec", "res"},
				new TypeInformation<?>[]{Types.STRING, VectorTypes.VECTOR})
		);
	}

}
