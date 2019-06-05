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

package org.apache.flink.ml.params.statistics.analysis;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.params.shared.colname.HasKeepColNames;
import org.apache.flink.ml.params.shared.colname.HasPredResultColName;
import org.apache.flink.ml.params.shared.colname.HasVectorColNameDvNull;

/**
 * Params of PCA prediction.
 */
public interface PcaPredictParams<T> extends
	HasKeepColNames <T>,
	HasPredResultColName <T>,
	HasVectorColNameDvNull <T> {

	ParamInfo <String> TRANSFORM_TYPE = ParamInfoFactory
		.createParamInfo("transformType", String.class)
		.setDescription("'SIMPLE' or 'SUBMEAN', SIMPLE is data * model, SUBMEAN is (data - mean) * model")
		.setHasDefaultValue("SIMPLE")
		.build();

	default String getTransformType() {
		return get(TRANSFORM_TYPE);
	}

	default T setTransformType(String value) {
		return set(TRANSFORM_TYPE, value);
	}
}
