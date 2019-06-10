/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.ml.api.core;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.WithParams;
import org.apache.flink.ml.util.param.ExtractParamInfosUtil;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base class for a stage in a pipeline. The interface is only a concept, and does not have any
 * actual functionality. Its subclasses must be either Estimator or Transformer. No other classes
 * should inherit this interface directly.
 *
 * <p>Each pipeline stage is with parameters, and requires a public empty constructor for
 * restoration in Pipeline.
 *
 * @param <T> The class type of the PipelineStage implementation itself, used by {@link
 *            org.apache.flink.ml.api.misc.param.WithParams}
 * @see WithParams
 */
interface PipelineStage<T extends PipelineStage<T>> extends WithParams<T>, Serializable {

	default String toJson() {
		return getParams().toJson();
	}

	default void loadJson(String json) {
		List<ParamInfo> paramInfos = ExtractParamInfosUtil.extractParamInfos(this);
		Map<String, Class<?>> classMap = new HashMap<>();
		for (ParamInfo i : paramInfos) {
			classMap.put(i.getName(), i.getValueClass());
		}
		getParams().loadJson(json, classMap);
	}
}
