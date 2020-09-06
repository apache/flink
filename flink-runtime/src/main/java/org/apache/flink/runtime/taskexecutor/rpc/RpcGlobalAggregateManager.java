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

package org.apache.flink.runtime.taskexecutor.rpc;

import java.io.IOException;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.util.InstantiationUtil;

public class RpcGlobalAggregateManager implements GlobalAggregateManager {

	private final JobMasterGateway jobMasterGateway;

	public RpcGlobalAggregateManager(JobMasterGateway jobMasterGateway) {
		this.jobMasterGateway = jobMasterGateway;
	}

	@Override
	public <IN, ACC, OUT> OUT updateGlobalAggregate(String aggregateName, Object aggregand, AggregateFunction<IN, ACC, OUT> aggregateFunction)
		throws IOException {
		ClosureCleaner.clean(aggregateFunction, ExecutionConfig.ClosureCleanerLevel.RECURSIVE,true);
		byte[] serializedAggregateFunction = InstantiationUtil.serializeObject(aggregateFunction);
		Object result = null;
		try {
			result = jobMasterGateway.updateGlobalAggregate(aggregateName, aggregand, serializedAggregateFunction).get();
		} catch (Exception e) {
			throw new IOException("Error updating global aggregate.", e);
		}
		return (OUT) result;
	}
}

