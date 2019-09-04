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
import org.apache.flink.ml.common.utils.RowCollector;
import org.apache.flink.ml.params.dataproc.vector.VectorToColumnsParams;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for VectorToColumnsMapper.
 */
public class VectorToColumnsMapperTest {
	@Test
	public void test1() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"vec"}, new TypeInformation<?>[]{Types.STRING});

		Params params = new Params()
			.set(VectorToColumnsParams.SELECTED_COL, "vec")
			.set(VectorToColumnsParams.OUTPUT_COLS, new String[]{"f0", "f1"});

		VectorToColumnsMapper mapper = new VectorToColumnsMapper(schema, params);
		RowCollector collector = new RowCollector();
		mapper.flatMap(Row.of(new DenseVector(new double[]{3.0, 4.0})), collector);
		List<Row> rows = collector.getRows();
		assertEquals(rows.get(0).getField(1), 3.0);
		assertEquals(rows.get(0).getField(2), 4.0);
		assertEquals(mapper.getOutputSchema(), new TableSchema(new String[]{"vec", "f0", "f1"},
			new TypeInformation<?>[]{Types.STRING, Types.DOUBLE, Types.DOUBLE}));

	}

	@Test
	public void test2() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"vec"}, new TypeInformation<?>[]{Types.STRING});

		Params params = new Params()
			.set(VectorToColumnsParams.SELECTED_COL, "vec")
			.set(VectorToColumnsParams.RESERVED_COLS, new String[]{})
			.set(VectorToColumnsParams.OUTPUT_COLS, new String[]{"f0", "f1"});

		VectorToColumnsMapper mapper = new VectorToColumnsMapper(schema, params);

		RowCollector collector = new RowCollector();
		mapper.flatMap(Row.of(new DenseVector(new double[]{3.0, 4.0})), collector);
		List<Row> rows = collector.getRows();
		assertEquals(rows.get(0).getField(0), 3.0);
		assertEquals(rows.get(0).getField(1), 4.0);
		assertEquals(mapper.getOutputSchema(), new TableSchema(new String[]{"f0", "f1"},
			new TypeInformation<?>[]{Types.DOUBLE, Types.DOUBLE}));
	}

	@Test
	public void test3() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"vec"}, new TypeInformation<?>[]{Types.STRING});

		Params params = new Params()
			.set(VectorToColumnsParams.SELECTED_COL, "vec")
			.set(VectorToColumnsParams.OUTPUT_COLS, new String[]{"f0", "f1", "f2"});

		VectorToColumnsMapper mapper = new VectorToColumnsMapper(schema, params);

		RowCollector collector = new RowCollector();
		mapper.flatMap(Row.of(new SparseVector(3, new int[]{1, 2}, new double[]{3.0, 4.0})), collector);
		List<Row> rows = collector.getRows();
		assertEquals(rows.get(0).getField(0), new SparseVector(3, new int[]{1, 2}, new double[]{3.0, 4.0}));
		assertEquals(rows.get(0).getField(1), 0.0);
		assertEquals(rows.get(0).getField(2), 3.0);
		assertEquals(rows.get(0).getField(3), 4.0);
		assertEquals(mapper.getOutputSchema(), new TableSchema(new String[]{"vec", "f0", "f1", "f2"},
			new TypeInformation<?>[]{Types.STRING, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE}));
	}
}
