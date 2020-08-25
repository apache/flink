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

package org.apache.flink.connector.hbase1;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.descriptors.AbstractHBaseValidator;

/**
 * The validator for HBase.
 * More features to be supported, e.g., batch read/write, async api(support from hbase version 2.0.0), Caching for LookupFunction.
 */
@Internal
public class HBaseValidator extends AbstractHBaseValidator {

	public static final String CONNECTOR_VERSION_VALUE_143 = "1.4.3";

	@Override
	protected boolean validateZkQuorum() {
		return false;
	}

	@Override
	protected String getConnectorVersion() {
		return CONNECTOR_VERSION_VALUE_143;
	}

	@Override
	protected boolean zkQuorumIsOptional() {
		return false;
	}
}
