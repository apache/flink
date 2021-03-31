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
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.common.linalg.DenseVector;
import org.apache.flink.ml.common.utils.VectorTypes;
import org.apache.flink.ml.params.dataproc.vector.VectorPolynomialExpandParams;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for OutputColsHelper.
 */
public class PolynomialExpansionMapperTest {
	@Test
	public void test1() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"vec"}, new TypeInformation<?>[]{Types.STRING()});

		Params params = new Params()
			.set(VectorPolynomialExpandParams.SELECTED_COL, "vec")
			.set(VectorPolynomialExpandParams.DEGREE, 2);

		PolynomialExpansionMapper mapper = new PolynomialExpansionMapper(schema, params);

		assertTrue(
			new DenseVector(new double[]{3.0, 9.0, 4.0, 12.0, 16.0})
				.equals(mapper.map(Row.of(new DenseVector(new double[]{3.0, 4.0}))).getField(0)));
		assertEquals(mapper.getOutputSchema(),
			new TableSchema(new String[]{"vec"}, new TypeInformation<?>[]{VectorTypes.VECTOR}));
	}

	@Test
	public void test2() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"vec"}, new TypeInformation<?>[]{Types.STRING()});

		Params params = new Params()
			.set(VectorPolynomialExpandParams.SELECTED_COL, "vec")
			.set(VectorPolynomialExpandParams.OUTPUT_COL, "res")
			.set(VectorPolynomialExpandParams.DEGREE, 2);

		PolynomialExpansionMapper mapper = new PolynomialExpansionMapper(schema, params);

		assertTrue(
			new DenseVector(new double[]{3.0, 9.0, 4.0, 12.0, 16.0})
				.equals(mapper.map(Row.of(new DenseVector(new double[]{3.0, 4.0}))).getField(1)));
		assertEquals(
			mapper.getOutputSchema(),
			new TableSchema(new String[]{"vec", "res"}, new TypeInformation<?>[]{Types.STRING(), VectorTypes.VECTOR})
		);
	}

	@Test
	public void test3() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"vec"}, new TypeInformation<?>[]{Types.STRING()});

		Params params = new Params()
			.set(VectorPolynomialExpandParams.SELECTED_COL, "vec")
			.set(VectorPolynomialExpandParams.OUTPUT_COL, "res")
			.set(VectorPolynomialExpandParams.RESERVED_COLS, new String[]{})
			.set(VectorPolynomialExpandParams.DEGREE, 2);

		PolynomialExpansionMapper mapper = new PolynomialExpansionMapper(schema, params);

		assertTrue(
			new DenseVector(new double[]{3.0, 9.0, 4.0, 12.0, 16.0})
				.equals(mapper.map(Row.of(new DenseVector(new double[]{3.0, 4.0}))).getField(0)));
		assertEquals(
			mapper.getOutputSchema(),
			new TableSchema(new String[]{"res"}, new TypeInformation<?>[]{VectorTypes.VECTOR})
		);
	}

	@Test
	public void test4() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"vec"}, new TypeInformation<?>[]{Types.STRING()});

		Params params = new Params()
			.set(VectorPolynomialExpandParams.SELECTED_COL, "vec")
			.set(VectorPolynomialExpandParams.OUTPUT_COL, "res")
			.set(VectorPolynomialExpandParams.DEGREE, 2);

		PolynomialExpansionMapper mapper = new PolynomialExpansionMapper(schema, params);
		assertTrue(
			new DenseVector(new double[]{2.0, 4.0, 3.0, 6.0, 9.0})
				.equals(mapper.map(Row.of(new DenseVector(new double[]{2.0, 3.0}))).getField(1)));
		assertEquals(
			mapper.getOutputSchema(),
			new TableSchema(new String[]{"vec", "res"}, new TypeInformation<?>[]{Types.STRING(), VectorTypes.VECTOR})
		);

	}

	@Test
	public void testGetPolySize() {
		int res1 = PolynomialExpansionMapper.getPolySize(4, 4);
		int res2 = PolynomialExpansionMapper.getPolySize(65, 2);
		assertEquals(res1, 70);
		assertEquals(res2, 2211);
	}

}
