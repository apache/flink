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

package org.apache.flink.ml.params.io;

import org.apache.flink.ml.params.BaseWithParam;
import org.apache.flink.ml.params.ParamInfo;

/**
 * Params for CSV sink oprator.
 */
public interface CsvSinkParams<T> extends BaseWithParam <T>,
	HasFilePath <T>, HasFieldDelimiterDvComma <T>, HasRowDelimiterDvNewline <T> {

	/**
	 * Param "numFiles" .
	 */
	ParamInfo <Integer> NUM_FILES = new ParamInfo <>(
		"numFiles",
		"num files",
		true, 1,
		Integer.class
	);

	default Integer getNumFiles() {
		return getParams().get(NUM_FILES);
	}

	default T setNumFiles(Integer value) {
		return set(NUM_FILES, value);
	}

}
