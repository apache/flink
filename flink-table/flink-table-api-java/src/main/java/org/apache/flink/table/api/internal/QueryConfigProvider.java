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

package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.PlannerConfig;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.delegation.Planner;

/**
 * An adapter to {@link PlannerConfig} that enables to pass {@link org.apache.flink.table.api.QueryConfig}
 * to {@link Planner} via {@link org.apache.flink.table.api.TableConfig}.
 */
@Internal
public class QueryConfigProvider implements PlannerConfig {
	private StreamQueryConfig config;

	public StreamQueryConfig getConfig() {
		return config;
	}

	public void setConfig(StreamQueryConfig config) {
		this.config = config;
	}
}
