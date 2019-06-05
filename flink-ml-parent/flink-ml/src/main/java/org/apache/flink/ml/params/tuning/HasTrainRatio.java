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

package org.apache.flink.ml.params.tuning;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.params.BaseWithParam;

/**
 * Ratio for training set and the validation set, range in (0, 1].
 */
public interface HasTrainRatio<T> extends BaseWithParam <T> {
	ParamInfo <Double> TRAIN_RATIO = ParamInfoFactory
		.createParamInfo("trainRatio", Double.class)
		.setDescription("Ratio for training set and the validation set, range in (0, 1].")
		.setHasDefaultValue(0.8)
		.build();

	default Double getTrainRatio() {
		return getParams().get(TRAIN_RATIO);
	}

	default T setTrainRatio(Double value) {
		return set(TRAIN_RATIO, value);
	}
}
