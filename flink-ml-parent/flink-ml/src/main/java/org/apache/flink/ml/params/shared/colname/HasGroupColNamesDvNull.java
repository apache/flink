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

import org.apache.flink.ml.params.BaseWithParam;
import org.apache.flink.ml.params.ParamInfo;

/**
 * Names of grouping columns.
 */
public interface HasGroupColNamesDvNull<T> extends BaseWithParam <T> {

	ParamInfo <String[]> GROUP_COL_NAMES = new ParamInfo <>(
		"groupColNames",
		"Names of grouping columns",
		true, null,
		String[].class
	);

	default String[] getGroupColNames() {
		return getParams().get(GROUP_COL_NAMES);
	}

	default T setGroupColNames(String... value) {
		return set(GROUP_COL_NAMES, value);
	}
}
