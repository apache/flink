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

package org.apache.flink.table.runtime.generated;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

/**
 * The base class for handling table aggregate functions.
 *
 * <p>It is code generated to handle all {@link TableAggregateFunction}s together in an aggregation.
 *
 * <p>It is the entry point for aggregate operators to operate all {@link TableAggregateFunction}s.
 */
public interface TableAggsHandleFunction extends AggsHandleFunctionBase {

	/**
	 * Emit the result of the table aggregation through the collector.
	 *
	 * @param out        the collector used to emit records.
	 * @param currentKey the current group key.
	 * @param isRetract  the retraction flag which indicates whether emit retract values.
	 */
	void emitValue(Collector<RowData> out, RowData currentKey, boolean isRetract) throws Exception;
}
