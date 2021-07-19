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

package org.apache.flink.ml.common.nlp;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.common.linalg.SparseVector;
import org.apache.flink.ml.common.utils.VectorTypes;
import org.apache.flink.ml.params.nlp.DocHashTFVectorizerParams;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for DocHashTFVectorizerMapper.
 */
public class DocHashTFVectorizerMapperTest {
	@Test
	public void test1() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"sentence"}, new TypeInformation<?>[]{Types.STRING});

		Params params = new Params()
			.set(DocHashTFVectorizerParams.SELECTED_COL, "sentence")
			.set(DocHashTFVectorizerParams.NUM_FEATURES, 10);

		DocHashTFVectorizerMapper mapper = new DocHashTFVectorizerMapper(schema, params);

		assertEquals(mapper.map(Row.of("This is a unit test for mapper")).getField(0),
			new SparseVector(10, new int[]{0, 2, 3, 4, 7}, new double[]{2.0, 2.0, 1.0, 1.0, 1.0}));
		assertEquals(mapper.getOutputSchema(),
			new TableSchema(new String[]{"sentence"}, new TypeInformation[]{VectorTypes.VECTOR}));
	}

	@Test
	public void test2() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"sentence"}, new TypeInformation<?>[]{Types.STRING});

		Params params = new Params()
			.set(DocHashTFVectorizerParams.SELECTED_COL, "sentence")
			.set(DocHashTFVectorizerParams.NUM_FEATURES, 20);

		DocHashTFVectorizerMapper mapper = new DocHashTFVectorizerMapper(schema, params);

		assertEquals(mapper.map(Row.of("This is a unit test for mapper")).getField(0),
			new SparseVector(20, new int[]{0, 7, 12, 13, 14}, new double[]{2.0, 1.0, 2.0, 1.0, 1.0}));
		assertEquals(mapper.getOutputSchema(),
			new TableSchema(new String[]{"sentence"}, new TypeInformation[]{VectorTypes.VECTOR}));
	}

	@Test
	public void test3() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"sentence"}, new TypeInformation<?>[]{Types.STRING});

		Params params = new Params()
			.set(DocHashTFVectorizerParams.SELECTED_COL, "sentence")
			.set(DocHashTFVectorizerParams.OUTPUT_COL, "output");

		DocHashTFVectorizerMapper mapper = new DocHashTFVectorizerMapper(schema, params);

		assertEquals(mapper.map(Row.of("This is a unit test for mapper")).getField(1),
			new SparseVector(262144, new int[]{28348, 101116, 119066, 145168, 168876, 207319, 225281},
				new double[]{1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0}));
		assertEquals(mapper.getOutputSchema(),
			new TableSchema(new String[]{"sentence", "output"},
				new TypeInformation<?>[]{Types.STRING, VectorTypes.VECTOR})
		);
	}

	@Test
	public void test4() throws Exception {
		TableSchema schema = new TableSchema(new String[]{"sentence"}, new TypeInformation<?>[]{Types.STRING});

		Params params = new Params()
			.set(DocHashTFVectorizerParams.SELECTED_COL, "sentence")
			.set(DocHashTFVectorizerParams.BINARY, true)
			.set(DocHashTFVectorizerParams.NUM_FEATURES, 10);

		DocHashTFVectorizerMapper mapper = new DocHashTFVectorizerMapper(schema, params);

		assertEquals(mapper.map(Row.of("This is a unit test for mapper")).getField(0),
			new SparseVector(10, new int[]{0, 2, 3, 4, 7}, new double[]{1.0, 1.0, 1.0, 1.0, 1.0}));
		assertEquals(mapper.getOutputSchema(),
			new TableSchema(new String[]{"sentence"}, new TypeInformation[]{VectorTypes.VECTOR}));
	}

}
