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
import org.apache.flink.ml.common.mapper.SISOMapper;
import org.apache.flink.ml.common.utils.VectorTypes;
import org.apache.flink.ml.params.nlp.DocHashTFVectorizerParams;
import org.apache.flink.table.api.TableSchema;

import org.apache.flink.shaded.guava18.com.google.common.hash.HashFunction;

import java.util.TreeMap;

import static org.apache.flink.shaded.guava18.com.google.common.hash.Hashing.murmur3_32;

/**
 * Transform a document to a sparse vector based on the frequency of every word in the string.
 *
 * <p>It uses MurmurHash 3 to get the hash value of a word as the index, and the frequency of the word as value.
 */
public class DocHashTFVectorizerMapper extends SISOMapper {
	private final boolean binary;
	private final int numFeatures;
	private static final HashFunction HASH = murmur3_32(0);

	public DocHashTFVectorizerMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		binary = this.params.get(DocHashTFVectorizerParams.BINARY);
		numFeatures = this.params.get(DocHashTFVectorizerParams.NUM_FEATURES);
	}

	@Override
	protected TypeInformation initOutputColType() {
		return VectorTypes.VECTOR;
	}

	@Override
	protected Object map(Object input) {
		if (null == input) {
			return null;
		}
		TreeMap<Integer, Double> keyValueMap = new TreeMap<>();

		String content = (String) input;

		String[] tokens = content.split(NLPConstant.WORD_DELIMITER);
		for (String word : tokens) {
			int hashValue = Math.abs(HASH.hashUnencodedChars(word).asInt());
			int index = Math.floorMod(hashValue, numFeatures);
			keyValueMap.compute(index, (k, v) -> (binary || v == null) ? 1 : v + 1);
		}

		return new SparseVector(numFeatures, keyValueMap);
	}
}
