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

package org.apache.flink.table.runtime.operators.python.utils;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * The collector is used to convert a {@link Row} to a {@link CRow}.
 */
public class StreamRecordCRowWrappingCollector implements Collector<Row> {

	private final Collector<StreamRecord<CRow>> out;
	private final CRow reuseCRow = new CRow();

	/**
	 * For Table API & SQL jobs, the timestamp field is not used.
	 */
	private final StreamRecord<CRow> reuseStreamRecord = new StreamRecord<>(reuseCRow);

	public StreamRecordCRowWrappingCollector(Collector<StreamRecord<CRow>> out) {
		this.out = out;
	}

	public void setChange(boolean change) {
		this.reuseCRow.change_$eq(change);
	}

	@Override
	public void collect(Row record) {
		reuseCRow.row_$eq(record);
		out.collect(reuseStreamRecord);
	}

	@Override
	public void close() {
		out.close();
	}
}
