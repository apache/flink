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

package org.apache.flink.graph.drivers.input;

import org.apache.flink.graph.drivers.parameter.LongParameter;
import org.apache.flink.graph.drivers.parameter.ParameterizedBase;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;

/**
 * Base class for inputs.
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public abstract class InputBase<K, VV, EV>
extends ParameterizedBase
implements Input<K, VV, EV> {

	protected LongParameter parallelism = new LongParameter(this, "__parallelism")
		.setDefaultValue(PARALLELISM_DEFAULT)
		.setMinimumValue(1)
		.setMaximumValue(Integer.MAX_VALUE);

	@Override
	public String getName() {
		return this.getClass().getSimpleName();
	}
}
