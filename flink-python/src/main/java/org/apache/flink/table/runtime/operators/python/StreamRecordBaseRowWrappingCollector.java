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

package org.apache.flink.table.runtime.operators.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.util.Collector;

/**
 * The collector is used to convert a {@link BaseRow} to a {@link StreamRecord}.
 */
@Internal
public final class StreamRecordBaseRowWrappingCollector implements Collector<BaseRow> {

	private final Collector<StreamRecord<BaseRow>> out;

	/**
	 * For Table API & SQL jobs, the timestamp field is not used.
	 */
	private final StreamRecord<BaseRow> reuseStreamRecord = new StreamRecord<>(null);

	StreamRecordBaseRowWrappingCollector(Collector<StreamRecord<BaseRow>> out) {
		this.out = out;
	}

	@Override
	public void collect(BaseRow record) {
		out.collect(reuseStreamRecord.replace(record));
	}

	@Override
	public void close() {
		out.close();
	}
}
