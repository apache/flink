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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.common.linalg.SparseVector;
import org.apache.flink.ml.common.mapper.SISOModelMapper;
import org.apache.flink.ml.common.utils.JsonConverter;
import org.apache.flink.ml.common.utils.VectorTypes;
import org.apache.flink.ml.params.nlp.DocCountVectorizerPredictParams;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Transform a document to a sparse vector with the statistics of DocCountVectorizerModel.
 *
 * <p>It supports several types: IDF/TF/TF_IDF/Binary/WordCount.
 */
public class DocCountVectorizerModelMapper extends SISOModelMapper {

	/**
	 * TypeReference for deserialize data from json string.
	 */
	private static final Type DATA_TUPLE4_TYPE = new TypeReference<Tuple4<String, Integer, Double, Integer>>() {
	}.getType();

	private enum FeatureType {
		/**
		 * IDF type, the output value is inverse document frequency.
		 */
		IDF(
			tuple4 -> Tuple2.of(tuple4.f3, tuple4.f2),
			(weight, termFrequency) -> weight
		),
		/**
		 * WORD_COUNT type, the output value is the word count.
		 */
		WORD_COUNT(
			tuple4 -> Tuple2.of(tuple4.f3, (double) tuple4.f1),
			(weight, termFrequency) -> weight
		),
		/**
		 * TF_IDF type, the output value is term frequency * inverse document frequency.
		 */
		TF_IDF(
			tuple4 -> Tuple2.of(tuple4.f3, tuple4.f2),
			(weight, termFrequency) -> weight * termFrequency
		),
		/**
		 * BINARY type, the output value is 1.0.
		 */
		BINARY(
			tuple4 -> Tuple2.of(tuple4.f3, 1.0),
			(weight, termFrequency) -> weight
		),
		/**
		 * TF type, the output value is term frequency.
		 */
		TF(
			tuple4 -> Tuple2.of(tuple4.f3, 1.0),
			(weight, termFrequency) -> weight * termFrequency
		);

		final Function<Tuple4<String, Integer, Double, Integer>, Tuple2<Integer, Double>> extractIdWeightFunc;
		final BiFunction<Double, Double, Double> featureValueFunc;

		FeatureType(Function<Tuple4<String, Integer, Double, Integer>, Tuple2<Integer, Double>> extractIdWeightFunc,
					BiFunction<Double, Double, Double> featureValueFunc) {
			this.extractIdWeightFunc = extractIdWeightFunc;
			this.featureValueFunc = featureValueFunc;
		}
	}

	private final double minTF;
	private final FeatureType featureType;
	private HashMap<String, Tuple2<Integer, Double>> wordIdWeight;
	private int featureNum;

	public DocCountVectorizerModelMapper(TableSchema modelScheme, TableSchema dataSchema, Params params) {
		super(modelScheme, dataSchema, params);
		this.featureType = FeatureType.valueOf(this.params.get(DocCountVectorizerPredictParams.FEATURE_TYPE).toUpperCase());
		this.minTF = this.params.get(DocCountVectorizerPredictParams.MIN_TF);
	}

	@Override
	protected TypeInformation initPredResultColType() {
		return VectorTypes.SPARSE_VECTOR;
	}

	@Override
	public void loadModel(List<Row> modelRows) {
		this.wordIdWeight = new HashMap<>(modelRows.size());
		List<String> data = new DocCountVectorizerModelDataConverter().load(modelRows);
		featureNum = data.size();
		for (String feature : data) {
			Tuple4<String, Integer, Double, Integer> t = JsonConverter.fromJson(feature, DATA_TUPLE4_TYPE);
			wordIdWeight.put(t.f0, featureType.extractIdWeightFunc.apply(t));
		}
	}

	@Override
	protected Object predictResult(Object input) {
		if (null == input) {
			return null;
		}
		HashMap<String, Integer> wordCount = new HashMap<>(0);
		String content = (String) input;
		String[] tokens = content.split(NLPConstant.WORD_DELIMITER);
		double minTermCount = this.minTF >= 1.0 ? this.minTF : this.minTF * tokens.length;
		double tokenRatio = 1.0 / tokens.length;

		for (String token : tokens) {
			if (wordIdWeight.containsKey(token)) {
				wordCount.merge(token, 1, Integer::sum);
			}
		}
		int[] indexes = new int[wordCount.size()];
		double[] values = new double[indexes.length];
		int pos = 0;
		for (Map.Entry<String, Integer> entry : wordCount.entrySet()) {
			double count = entry.getValue();
			if (count >= minTermCount) {
				Tuple2<Integer, Double> idWeight = wordIdWeight.get(entry.getKey());
				indexes[pos] = idWeight.f0;
				values[pos++] = featureType.featureValueFunc.apply(idWeight.f1, count * tokenRatio);
			}
		}
		return new SparseVector(featureNum, Arrays.copyOf(indexes, pos), Arrays.copyOf(values, pos));
	}
}

