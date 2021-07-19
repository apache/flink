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
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.common.linalg.SparseVector;
import org.apache.flink.ml.common.mapper.SISOModelMapper;
import org.apache.flink.ml.common.utils.VectorTypes;
import org.apache.flink.ml.params.nlp.DocHashIDFVectorizerPredictParams;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.guava18.com.google.common.hash.HashFunction;

import java.util.List;
import java.util.TreeMap;

import static org.apache.flink.shaded.guava18.com.google.common.hash.Hashing.murmur3_32;

/**
 * Transform a document to a sparse vector based on the inverse document frequency(idf) statistics provided by
 * DocHashIDFVectorizerTrainBatchOp.
 *
 * <p>It uses MurmurHash 3 to get the hash value of a word as the index, and the idf of the word as value.
 */
public class DocHashIDFVectorizerModelMapper extends SISOModelMapper {
	private DocHashIDFVectorizerModelData model;
	private final boolean binary;
	private static final HashFunction HASH = murmur3_32(0);

	public DocHashIDFVectorizerModelMapper(TableSchema modelScheme, TableSchema dataSchema, Params params) {
		super(modelScheme, dataSchema, params);
		this.binary = params.get(DocHashIDFVectorizerPredictParams.BINARY);
	}

	@Override
	public void loadModel(List<Row> modelRows) {
		this.model = new DocHashIDFVectorizerModelDataConverter().load(modelRows);
	}

	@Override
	protected TypeInformation initPredResultColType() {
		return VectorTypes.SPARSE_VECTOR;
	}

	@Override
	protected Object predictResult(Object input) {
		if (null == input) {
			return null;
		}
		TreeMap<Integer, Double> keyValueMap = new TreeMap<>();
		String content = (String) input;
		String[] tokens = content.split(NLPConstant.WORD_DELIMITER);
		for (String word : tokens) {
			int hashValue = Math.abs(HASH.hashUnencodedChars(word).asInt());
			int index = Math.floorMod(hashValue, model.numFeatures);
			Double value = model.idfMap.get(index);
			if (null != value) {
				keyValueMap.compute(index, (k, v) -> (v == null || binary) ? value : v + value);
			}
		}
		return new SparseVector(model.numFeatures, keyValueMap);
	}
}
