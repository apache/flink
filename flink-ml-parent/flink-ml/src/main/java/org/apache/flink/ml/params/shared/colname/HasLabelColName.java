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
 * Name of the label column in the input table.
 */
public interface HasLabelColName<T> extends BaseWithParam <T> {

	ParamInfo <String> LABEL_COL_NAME = new ParamInfo <>(
		"labelColName",
		"Name of the label column in the input table",
		false,
		String.class
	);

	default String getLabelColName() {
		return getParams().get(LABEL_COL_NAME);
	}

	default T setLabelColName(String value) {
		return set(LABEL_COL_NAME, value);
	}
}
