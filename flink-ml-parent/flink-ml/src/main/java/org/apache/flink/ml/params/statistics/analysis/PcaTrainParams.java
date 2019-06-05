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
import org.apache.flink.ml.params.shared.colname.HasSelectedColNamesDvNull;
import org.apache.flink.ml.params.shared.colname.HasVectorColNameDvNull;

/**
 * Params of PCA training.
 */
public interface PcaTrainParams<T> extends
	HasSelectedColNamesDvNull <T>,
	HasVectorColNameDvNull <T> {

	ParamInfo <Integer> K = ParamInfoFactory
		.createParamInfo("k", Integer.class)
		.setDescription("the value of K.")
		.setRequired()
		.setAlias(new String[] {"p"})
		.build();
	ParamInfo <String> CALCULATION_TYPE = ParamInfoFactory
		.createParamInfo("calculationType", String.class)
		.setDescription("compute type, be CORR, COV_SAMPLE, COVAR_POP.")
		.setHasDefaultValue("CORR")
		.setAlias(new String[] {"calcType", "pcaType"})
		.build();

	default Integer getK() {
		return getParams().get(K);
	}

	default T setK(Integer value) {
		return set(K, value);
	}

	default String getCalculationType() {
		return getParams().get(CALCULATION_TYPE);
	}

	default T setCalculationType(String value) {
		return set(CALCULATION_TYPE, value);
	}
}
