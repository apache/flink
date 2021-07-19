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
import org.apache.flink.ml.params.nlp.DocCountVectorizerPredictParams;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for DocCountVectorizer.
 */
public class DocCountVectorizerModelMapperTest {
	private Row[] rows = new Row[]{
		Row.of(0L, "{\"modelSchema\":\"\\\"model_id bigint,model_info string\\\"\",\"isNewFormat\":\"true\"}"),
		Row.of(1048576L, "{\"f0\":\"i\",\"f1\":2,\"f2\":0.6931471805599453,\"f3\":6}"),
		Row.of(2097152L, "{\"f0\":\"e\",\"f1\":4,\"f2\":0.1823215567939546,\"f3\":2}"),
		Row.of(3145728L, "{\"f0\":\"a\",\"f1\":6,\"f2\":0.4054651081081644,\"f3\":0}"),
		Row.of(4194304L, "{\"f0\":\"b\",\"f1\":6,\"f2\":0.1823215567939546,\"f3\":1}"),
		Row.of(5242880L, "{\"f0\":\"c\",\"f1\":2,\"f2\":0.6931471805599453,\"f3\":7}"),
		Row.of(6291456L, "{\"f0\":\"h\",\"f1\":3,\"f2\":0.4054651081081644,\"f3\":3}"),
		Row.of(7340032L, "{\"f0\":\"d\",\"f1\":2,\"f2\":0.6931471805599453,\"f3\":4}"),
		Row.of(8388608L, "{\"f0\":\"j\",\"f1\":2,\"f2\":0.6931471805599453,\"f3\":5}"),
		Row.of(9437184L, "{\"f0\":\"g\",\"f1\":2,\"f2\":0.6931471805599453,\"f3\":8}"),
		Row.of(10485760L, "{\"f0\":\"n\",\"f1\":1,\"f2\":1.0986122886681098,\"f3\":9}"),
		Row.of(11534336L, "{\"f0\":\"f\",\"f1\":1,\"f2\":1.0986122886681098,\"f3\":10}")
	};
	private List<Row> model = Arrays.asList(rows);
	private TableSchema modelSchema = new TableSchema(new String[]{"model_id", "model_info"},
		new TypeInformation[]{Types.LONG, Types.STRING});

	@Test
	public void testWordCountType() throws Exception {
		TableSchema dataSchema = new TableSchema(new String[]{"sentence"}, new TypeInformation<?>[]{Types.STRING});

		Params params = new Params()
			.set(DocCountVectorizerPredictParams.SELECTED_COL, "sentence")
			.set(DocCountVectorizerPredictParams.FEATURE_TYPE, "Word_Count");

		DocCountVectorizerModelMapper mapper = new DocCountVectorizerModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals(mapper.map(Row.of("a b c d e")).getField(0),
			new SparseVector(11, new int[]{0, 1, 2, 4, 7}, new double[]{6.0, 6.0, 4.0, 2.0, 2.0}));
		assertEquals(mapper.getOutputSchema(),
			new TableSchema(new String[]{"sentence"}, new TypeInformation[]{VectorTypes.SPARSE_VECTOR}));
	}

	@Test
	public void testTFIDFType() throws Exception {
		TableSchema dataSchema = new TableSchema(new String[]{"sentence"}, new TypeInformation<?>[]{Types.STRING});

		Params params = new Params()
			.set(DocCountVectorizerPredictParams.SELECTED_COL, "sentence")
			.set(DocCountVectorizerPredictParams.FEATURE_TYPE, "TF_IDF");

		DocCountVectorizerModelMapper mapper = new DocCountVectorizerModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals(mapper.map(Row.of("a b c d e")).getField(0),
			new SparseVector(11, new int[]{0, 1, 2, 4, 7},
				new double[]{0.08109302162163289, 0.03646431135879092, 0.03646431135879092, 0.13862943611198905,
					0.13862943611198905}));
		assertEquals(mapper.getOutputSchema(),
			new TableSchema(new String[]{"sentence"}, new TypeInformation[]{VectorTypes.SPARSE_VECTOR}));
	}

	@Test
	public void testOutputCol() throws Exception {
		TableSchema dataSchema = new TableSchema(new String[]{"sentence"}, new TypeInformation<?>[]{Types.STRING});

		Params params = new Params()
			.set(DocCountVectorizerPredictParams.SELECTED_COL, "sentence")
			.set(DocCountVectorizerPredictParams.OUTPUT_COL, "output")
			.set(DocCountVectorizerPredictParams.FEATURE_TYPE, "TF");

		DocCountVectorizerModelMapper mapper = new DocCountVectorizerModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals(mapper.map(Row.of("a b c d e")).getField(1),
			new SparseVector(11, new int[]{0, 1, 2, 4, 7}, new double[]{0.2, 0.2, 0.2, 0.2, 0.2}));
		assertEquals(mapper.getOutputSchema(),
			new TableSchema(new String[]{"sentence", "output"},
				new TypeInformation[]{Types.STRING, VectorTypes.SPARSE_VECTOR}));
	}

	@Test
	public void testMinTF() throws Exception {
		TableSchema dataSchema = new TableSchema(new String[]{"sentence"}, new TypeInformation<?>[]{Types.STRING});

		Params params = new Params()
			.set(DocCountVectorizerPredictParams.SELECTED_COL, "sentence")
			.set(DocCountVectorizerPredictParams.FEATURE_TYPE, "Binary")
			.set(DocCountVectorizerPredictParams.MIN_TF, 0.2);

		DocCountVectorizerModelMapper mapper = new DocCountVectorizerModelMapper(modelSchema, dataSchema, params);
		mapper.loadModel(model);

		assertEquals(mapper.map(Row.of("a b c d e a a b e")).getField(0),
			new SparseVector(11, new int[]{0, 1, 2}, new double[]{1.0, 1.0, 1.0}));
		assertEquals(mapper.map(Row.of("a b c d")).getField(0),
			new SparseVector(11, new int[]{0, 1, 4, 7}, new double[]{1.0, 1.0, 1.0, 1.0}));
		assertEquals(mapper.getOutputSchema(),
			new TableSchema(new String[]{"sentence"}, new TypeInformation[]{VectorTypes.SPARSE_VECTOR}));
	}
}
