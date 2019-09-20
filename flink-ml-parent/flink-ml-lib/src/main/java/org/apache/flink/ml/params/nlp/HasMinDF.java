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
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Params minDF.When the number of documents a word appears in is below minDF, the word will not be included in the
 * dictionary.
 *
 * <p>It could be an exact count or a fraction of the document number count. When minDF is within [0, 1), it's used as a fraction.
 */
public interface HasMinDF<T> extends WithParams<T> {
	ParamInfo<Double> MIN_DF = ParamInfoFactory
		.createParamInfo("minDF", Double.class)
		.setDescription(
			"When the number of documents a word appears in is below minDF, the word will not be included in the "
				+ "dictionary. It could be an exact count"
				+ "or a fraction of the document number count. When minDF is within [0, 1), it's used as a fraction.")
		.setHasDefaultValue(1.0)
		.build();

	default double getMinDF() {
		return get(MIN_DF);
	}

	default T setMinDF(Double value) {
		return set(MIN_DF, value);
	}
}
