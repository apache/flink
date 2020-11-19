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

package org.apache.flink.table.runtime.operators.deduplicate;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.runtime.operators.deduplicate.DeduplicateFunctionHelper.processFirstRowOnProcTime;

/**
 * This function is used to deduplicate on keys and keeps only first row.
 */
public class ProcTimeDeduplicateKeepFirstRowFunction
		extends DeduplicateFunctionBase<Boolean, RowData, RowData, RowData> {

	private static final long serialVersionUID = 5865777137707602549L;

	// state stores a boolean flag to indicate whether key appears before.
	public ProcTimeDeduplicateKeepFirstRowFunction(long stateRetentionTime) {
		super(Types.BOOLEAN, null, stateRetentionTime);
	}

	@Override
	public void processElement(RowData input, Context ctx, Collector<RowData> out) throws Exception {
		processFirstRowOnProcTime(input, state, out);
	}
}
