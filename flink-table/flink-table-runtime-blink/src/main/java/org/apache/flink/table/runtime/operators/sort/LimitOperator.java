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

package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.util.Collector;

/**
 * Operator for batch limit.
 * TODO support stopEarly.
 */
public class LimitOperator extends TableStreamOperator<BaseRow>
		implements OneInputStreamOperator<BaseRow, BaseRow> {

	private final boolean isGlobal;
	private final long limitStart;
	private final long limitEnd;

	private transient Collector<BaseRow> collector;
	private transient int count = 0;

	public LimitOperator(boolean isGlobal, long limitStart, long limitEnd) {
		this.isGlobal = isGlobal;
		this.limitStart = limitStart;
		this.limitEnd = limitEnd;
	}

	@Override
	public void open() throws Exception {
		super.open();
		this.collector = new StreamRecordCollector<>(output);
	}

	@Override
	public void processElement(StreamRecord<BaseRow> element) throws Exception {
		if (count < limitEnd) {
			count++;
			if (!isGlobal || count > limitStart) {
				collector.collect(element.getValue());
			}
		}
	}
}
