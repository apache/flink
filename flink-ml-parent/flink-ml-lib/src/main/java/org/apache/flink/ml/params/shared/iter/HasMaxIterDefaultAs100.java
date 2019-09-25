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

package org.apache.flink.ml.params.shared.iter;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * An interface for classes with a parameter specifying the maximum iterations.
 */
public interface HasMaxIterDefaultAs100<T> extends WithParams<T> {
	ParamInfo<Integer> MAX_ITER = ParamInfoFactory
		.createParamInfo("maxIter", Integer.class)
		.setDescription("Maximum iterations, The default value is 100")
		.setHasDefaultValue(100)
		.setAlias(new String[]{"maxIteration"})
		.build();

	default Integer getMaxIter() {
		return get(MAX_ITER);
	}

	default T setMaxIter(Integer value) {
		return set(MAX_ITER, value);
	}
}
