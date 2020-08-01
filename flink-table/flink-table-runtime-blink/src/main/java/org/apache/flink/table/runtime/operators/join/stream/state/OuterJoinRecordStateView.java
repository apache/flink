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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.RowData;

/**
 * A {@link OuterJoinRecordStateView} is an extension to {@link JoinRecordStateView}.
 * The {@link OuterJoinRecordStateView} is used to store records for the outer input
 * side of the Join, e.g. the left side of left join, the both side of full join.
 *
 * <p>The additional information we should store with the record is the number of associations
 * which is the number of records associated this record with other side. This is an
 * important information when to send/retract a null padding row, to avoid recompute the
 * associated numbers every time.
 *
 * @see JoinRecordStateView
 */
public interface OuterJoinRecordStateView extends JoinRecordStateView {

	/**
	 * Adds a new record with the number of associations to the state view.
	 * @param record the added record
	 * @param numOfAssociations the number of records associated with other side
	 */
	void addRecord(RowData record, int numOfAssociations) throws Exception;

	/**
	 * Updates the number of associations belongs to the record.
	 * @param record the record to update
	 * @param numOfAssociations the new number of records associated with other side
	 */
	void updateNumOfAssociations(RowData record, int numOfAssociations) throws Exception;

	/**
	 * Gets all the records and number of associations under the current context (i.e. join key).
	 */
	Iterable<Tuple2<RowData, Integer>> getRecordsAndNumOfAssociations() throws Exception;
}
