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

package org.apache.flink.table.runtime.operators.join.stream.state;

import org.apache.flink.table.data.RowData;

/**
 * A {@link JoinRecordStateView} is a view to the join state. It encapsulates the join state and
 * provides some APIs facing the input records. The join state is used to store
 * input records. The structure of the join state is vary depending on the {@link JoinInputSideSpec}.
 *
 * <p>For example: when the {@link JoinInputSideSpec} is JoinKeyContainsUniqueKey, we will use
 * {@link org.apache.flink.api.common.state.ValueState} to store records which has better performance.
 */
public interface JoinRecordStateView {

	/**
	 * Add a new record to the state view.
	 */
	void addRecord(RowData record) throws Exception;

	/**
	 * Retract the record from the state view.
	 */
	void retractRecord(RowData record) throws Exception;

	/**
	 * Gets all the records under the current context (i.e. join key).
	 */
	Iterable<RowData> getRecords() throws Exception;
}
