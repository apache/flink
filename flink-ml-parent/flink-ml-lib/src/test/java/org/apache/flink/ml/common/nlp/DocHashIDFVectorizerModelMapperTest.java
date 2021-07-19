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
import org.apache.flink.ml.params.nlp.DocHashIDFVectorizerPredictParams;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for DocHashIDFVectorizerModelMapper.
 */
public class DocHashIDFVectorizerModelMapperTest {
	private Row[] rows = new Row[]{
		Row.of(0L, "{\"numFeatures\":\"20\"}"),
		Row.of(1048576L, "{\"16\":0.4054651081081644,\"7\":0.0,\"13\":0.4054651081081644,\"14\":-0.5108256237659907,"
			+ "\"15\":-0.2876820724517809}")
	};
	private List<Row> model = Arrays.asList(rows);
	private TableSchema modelSchema = new DocHashIDFVectorizerModelDataConverter().getModelSchema();

	@Test
	public void testDefault() throws Exception {
		TableSchema dataSchema = new TableSchema(new String[]{"sentence"}, new TypeInformation<?>[]{Types.STRING});

		Params params = new Params()
			.set(DocHashIDFVectorizerPredictParams.SELECTED_COL, "sentence");

		DocHashIDFVectorizerModelMapper mapper = new DocHashIDFVectorizerModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals(mapper.map(Row.of("a b c d a a ")).getField(0),
			new SparseVector(20, new int[]{7, 13, 14, 15},
				new double[]{0.0, 0.4054651081081644, -1.5324768712979722, -0.2876820724517809}));
		assertEquals(mapper.getOutputSchema(),
			new TableSchema(new String[]{"sentence"}, new TypeInformation[]{VectorTypes.SPARSE_VECTOR}));
	}

	@Test
	public void testBinary() throws Exception {
		TableSchema dataSchema = new TableSchema(new String[]{"sentence"}, new TypeInformation<?>[]{Types.STRING});

		Params params = new Params()
			.set(DocHashIDFVectorizerPredictParams.SELECTED_COL, "sentence")
			.set(DocHashIDFVectorizerPredictParams.BINARY, true);

		DocHashIDFVectorizerModelMapper mapper = new DocHashIDFVectorizerModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals(mapper.map(Row.of("a b c d a a ")).getField(0),
			new SparseVector(20, new int[]{7, 13, 14, 15},
				new double[]{0.0, 0.4054651081081644, -0.5108256237659907, -0.2876820724517809}));
		assertEquals(mapper.getOutputSchema(),
			new TableSchema(new String[]{"sentence"}, new TypeInformation[]{VectorTypes.SPARSE_VECTOR}));
	}

}
