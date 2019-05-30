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
 * Random seed, it should be positive integer.
 */
public interface HasRandomSeed<T> extends BaseWithParam <T> {
	ParamInfo <Long> RANDOM_SEED = new ParamInfo <>(
		"randomSeed",
		new String[] {"seed"},
		"Random seed, it should be positive integer",
		true, 772209414L,
		Long.class
	);

	default Long getRandomSeed() {
		return getParams().get(RANDOM_SEED);
	}

	default T setRandomSeed(Long value) {
		return set(RANDOM_SEED, value);
	}
}
