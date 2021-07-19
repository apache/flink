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

package org.apache.flink.ml.params.ml.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.params.mapper.SISOMapperParams;

/**
 * Params for binarizer.
 */
public interface BinarizerParams<T> extends
	SISOMapperParams<T> {

	ParamInfo<Double> THRESHOLD = ParamInfoFactory
		.createParamInfo("threshold", Double.class)
		.setDescription(
			"Binarization threshold, when number is greater than or equal to threshold, it will be set 1.0, else 0.0.")
		.setHasDefaultValue(0.0)
		.build();

	default Double getThreshold() {
		return get(THRESHOLD);
	}

	default T setThreshold(Double value) {
		return set(THRESHOLD, value);
	}

}
