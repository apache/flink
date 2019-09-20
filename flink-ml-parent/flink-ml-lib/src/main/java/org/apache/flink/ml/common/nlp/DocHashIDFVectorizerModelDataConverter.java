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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.common.model.SimpleModelDataConverter;
import org.apache.flink.ml.common.utils.JsonConverter;
import org.apache.flink.ml.params.nlp.DocHashIDFVectorizerTrainParams;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;

import java.util.Collections;
import java.util.HashMap;

/**
 * ModelDataConverter for DocHashIDFVectorizer.
 **/
public class DocHashIDFVectorizerModelDataConverter
	extends SimpleModelDataConverter<DocHashIDFVectorizerModelData, DocHashIDFVectorizerModelData> {
	public DocHashIDFVectorizerModelDataConverter() {
	}

	@Override
	public Tuple2<Params, Iterable<String>> serializeModel(DocHashIDFVectorizerModelData data) {
		Params meta = new Params()
			.set(DocHashIDFVectorizerTrainParams.NUM_FEATURES, data.numFeatures);
		return Tuple2.of(meta, Collections.singletonList(JsonConverter.toJson(data.idfMap)));
	}

	@Override
	public DocHashIDFVectorizerModelData deserializeModel(Params meta, Iterable<String> data) {
		String modelString = data.iterator().next();
		DocHashIDFVectorizerModelData modelData = new DocHashIDFVectorizerModelData();
		modelData.idfMap = JsonConverter.fromJson(modelString,
			new TypeReference<HashMap<Integer, Double>>() {
			}.getType());
		modelData.numFeatures = meta.get(DocHashIDFVectorizerTrainParams.NUM_FEATURES);
		return modelData;
	}
}
