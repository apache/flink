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

package org.apache.flink.ml.params.nlp;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.params.mapper.SISOModelMapperParams;

/**
 * Params for DocCountVectorizerPredict.
 */
public interface DocCountVectorizerPredictParams<T> extends
	SISOModelMapperParams<T> {

	ParamInfo<String> FEATURE_TYPE = ParamInfoFactory
		.createParamInfo("featureType", String.class)
		.setDescription("Feature type, support IDF/WORD_COUNT/TF_IDF/Binary/TF")
		.setRequired()
		.build();

	ParamInfo<Double> MIN_TF = ParamInfoFactory
		.createParamInfo("minTF", Double.class)
		.setDescription("When the number word in this document in is below minTF, the word will be ignored. It could be an exact "
			+ "count or a fraction of the document token count. When minTF is within [0, 1), it's used as a fraction.")
		.setHasDefaultValue(1.0)
		.build();

	default String getFeatureType() {
		return get(FEATURE_TYPE);
	}

	default T setFeatureType(String value) {
		return set(FEATURE_TYPE, value);
	}

	default double getMinTF() {
		return get(MIN_TF);
	}

	default T setMinTF(Double value) {
		return set(MIN_TF, value);
	}
}
