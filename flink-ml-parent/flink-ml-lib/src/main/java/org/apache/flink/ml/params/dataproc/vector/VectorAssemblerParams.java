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
import org.apache.flink.ml.params.mapper.MISOMapperParams;

/**
 * parameters of vector assembler.
 */
public interface VectorAssemblerParams<T> extends
	MISOMapperParams<T> {

	ParamInfo<String> HANDLE_INVALID = ParamInfoFactory
		.createParamInfo("handleInvalid", String.class)
		.setDescription("parameter for how to handle invalid data (NULL values)")
		.setHasDefaultValue("error")
		.build();

	/**
	 * parameter how to handle invalid data (NULL values). Options are 'skip' (filter out rows with
	 * invalid data), 'error' (throw an error), or 'keep' (return relevant number of NaN in the output).
	 */
	default String getHandleInvalid() {
		return get(HANDLE_INVALID);
	}

	default T setHandleInvalid(String value) {
		return set(HANDLE_INVALID, value);
	}
}
