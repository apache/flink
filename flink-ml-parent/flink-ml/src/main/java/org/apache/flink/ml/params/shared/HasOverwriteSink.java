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

package org.apache.flink.ml.params.shared;

import org.apache.flink.ml.params.BaseWithParam;
import org.apache.flink.ml.params.ParamInfo;

/**
 * If this parameter is true, it will remove the old data before write new data.
 */
public interface HasOverwriteSink<T> extends BaseWithParam <T> {
	ParamInfo <Boolean> OVERWRITE_SINK = new ParamInfo <>(
		"overwriteSink",
		"If this parameter is true, it will remove the old data before write new data",
		true, false,
		Boolean.class
	);

	default Boolean getOverwriteSink() {
		return getParams().get(OVERWRITE_SINK);
	}

	default T setOverwriteSink(Boolean value) {
		return set(OVERWRITE_SINK, value);
	}
}
