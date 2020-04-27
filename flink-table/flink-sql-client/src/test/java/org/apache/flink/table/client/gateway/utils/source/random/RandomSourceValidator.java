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

package org.apache.flink.table.client.gateway.utils.source.random;

import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

/**
 * Validator for {@link RandomSource}.
 */
public class RandomSourceValidator extends ConnectorDescriptorValidator {

	public static final String CONNECTOR_TYPE_VALUE = "random";
	public static final String RANDOM_LIMIT = "random.limit";
	public static final String RANDOM_INTERVAL = "random.interval";
	// this default value indicates that the random source is an unbounded source
	public static final int RANDOM_LIMIT_DEFAULT_VALUE = -1;
	public static final long RANDOM_INTERVAL_DEFAULT_VALUE = -1;

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateValue(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE, false);
		properties.validateInt(RANDOM_LIMIT, true, 1);
		properties.validateLong(RANDOM_INTERVAL, true, 1);
	}
}
