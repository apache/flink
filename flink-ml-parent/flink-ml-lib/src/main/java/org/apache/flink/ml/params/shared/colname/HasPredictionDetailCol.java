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

package org.apache.flink.ml.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * An interface for classes with a parameter specifying the column name of prediction detail.
 *
 * <p>The detail is the information of prediction result, such as the probability of each label in classifier.
 */
public interface HasPredictionDetailCol<T> extends WithParams <T> {

	ParamInfo <String> PREDICTION_DETAIL_COL = ParamInfoFactory
		.createParamInfo("predictionDetailCol", String.class)
		.setDescription("Column name of prediction result, it will include detailed info.")
		.build();

	default String getPredictionDetailCol() {
		return get(PREDICTION_DETAIL_COL);
	}

	default T setPredictionDetailCol(String value) {
		return set(PREDICTION_DETAIL_COL, value);
	}
}
