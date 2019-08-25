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

package org.apache.flink.runtime.taskexecutor;

import java.io.IOException;
import org.apache.flink.api.common.functions.AggregateFunction;


/**
 * This interface gives access to transient, named, global aggregates.  This can be sued to share
 * state amongst parallel tasks in a job.  It is not designed for high throughput updates
 * and the aggregates do NOT survive a job failure.  Each call to the updateGlobalAggregate()
 * method results in serialized RPC communication with the JobMaster so use with care.
 */
public interface GlobalAggregateManager {
	/**
	 * Update the global aggregate and return the new value.
	 *
	 * @param aggregateName The name of the aggregate to update
	 * @param aggregand The value to add to the aggregate
	 * @param aggregateFunction The function to apply to the current aggregate and aggregand to
	 * obtain the new aggregate value
	 * @return The updated aggregate
	 */
	<IN, ACC, OUT> OUT updateGlobalAggregate(String aggregateName, Object aggregand, AggregateFunction<IN, ACC, OUT> aggregateFunction) throws IOException;
}
