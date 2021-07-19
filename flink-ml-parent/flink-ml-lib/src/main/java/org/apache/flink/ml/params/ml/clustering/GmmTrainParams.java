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

package org.apache.flink.ml.params.ml.clustering;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;
import org.apache.flink.ml.params.shared.clustering.HasKDefaultAs2;
import org.apache.flink.ml.params.shared.colname.HasVectorCol;
import org.apache.flink.ml.params.shared.iter.HasMaxIterDefaultAs100;

/**
 * Parameters for Gaussian Mixture Model training.
 *
 * @param <T> The class that implement this interface.
 */
public interface GmmTrainParams<T> extends WithParams<T>,
	HasVectorCol<T>,
	HasKDefaultAs2<T>,
	HasMaxIterDefaultAs100<T> {

	ParamInfo<Double> TOL = ParamInfoFactory
		.createParamInfo("tol", Double.class)
		.setDescription("Iteration tolerance.")
		.setHasDefaultValue(0.01)
		.build();

	default Double getTol() {
		return get(TOL);
	}

	default T setTol(Double value) {
		return set(TOL, value);
	}
}
