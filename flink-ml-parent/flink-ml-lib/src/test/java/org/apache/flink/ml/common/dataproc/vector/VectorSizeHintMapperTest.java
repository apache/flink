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
import org.apache.flink.ml.common.utils.RowCollector;
import org.apache.flink.ml.common.utils.VectorTypes;
import org.apache.flink.ml.params.dataproc.vector.VectorSizeHintParams;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for VectorSizeHintMapper.
 */
public class VectorSizeHintMapperTest {
	@Test
	public void test1() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"vec"}, new TypeInformation <?>[] {Types.STRING});

		Params params = new Params()
			.set(VectorSizeHintParams.SELECTED_COL, "vec")
			.set(VectorSizeHintParams.SIZE, 3);

		VectorSizeHintMapper mapper = new VectorSizeHintMapper(schema, params);
		RowCollector output = new RowCollector();
		mapper.flatMap(Row.of(new DenseVector(new double[]{3.0, 4.0, 3.0})), output);
		assertEquals(output.getRows().size(), 1);
		assertEquals(mapper.getOutputSchema(),
			new TableSchema(new String[] {"vec"}, new TypeInformation <?>[] {VectorTypes.VECTOR}));
	}

	@Test
	public void test2() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"vec"}, new TypeInformation <?>[] {Types.STRING});

		Params params = new Params()
			.set(VectorSizeHintParams.SELECTED_COL, "vec")
			.set(VectorSizeHintParams.RESERVED_COLS, new String[] {})
			.set(VectorSizeHintParams.OUTPUT_COL, "res")
			.set(VectorSizeHintParams.HANDLE_INVALID, "skip")
			.set(VectorSizeHintParams.SIZE, 2);

		VectorSizeHintMapper mapper = new VectorSizeHintMapper(schema, params);
		RowCollector output = new RowCollector();
		mapper.flatMap(Row.of(new DenseVector(new double[]{3.0, 4.0, 3.0})), output);
		assertEquals(output.getRows().size(), 0);
		assertEquals(mapper.getOutputSchema(),
			new TableSchema(new String[] {"res"}, new TypeInformation <?>[] {VectorTypes.VECTOR}));

	}

	@Test
	public void test3() throws Exception {
		TableSchema schema = new TableSchema(new String[] {"vec"}, new TypeInformation <?>[] {Types.STRING});

		Params params = new Params()
			.set(VectorSizeHintParams.SELECTED_COL, "vec")
			.set(VectorSizeHintParams.OUTPUT_COL, "res")
			.set(VectorSizeHintParams.HANDLE_INVALID, "optimistic")
			.set(VectorSizeHintParams.SIZE, 2);

		VectorSizeHintMapper mapper = new VectorSizeHintMapper(schema, params);
		RowCollector output = new RowCollector();
		mapper.flatMap(Row.of(new DenseVector(new double[]{3.0, 4.0, 3.0})), output);
		assertEquals(output.getRows().size(), 1);
		assertEquals(mapper.getOutputSchema(),
			new TableSchema(new String[] {"vec", "res"},
				new TypeInformation <?>[] {Types.STRING, VectorTypes.VECTOR}));
	}
}
