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

package org.apache.flink.ml.params.dataproc.vector;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.params.mapper.SISOMapperParams;

/**
 * parameters of vector element wise product.
 */
public interface VectorElementwiseProductParams<T> extends
	SISOMapperParams<T> {

	ParamInfo<String> SCALING_VECTOR = ParamInfoFactory
		.createParamInfo("scalingVector", String.class)
		.setDescription("scaling vector with str format")
		.setRequired()
		.build();

	default String getScalingVector() {
		return get(SCALING_VECTOR);
	}

	default T setScalingVector(String value) {
		return set(SCALING_VECTOR, value);
	}
}
