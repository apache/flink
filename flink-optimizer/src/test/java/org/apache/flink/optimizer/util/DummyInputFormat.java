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

package org.apache.flink.optimizer.util;

import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.record.io.DelimitedInputFormat;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;

public final class DummyInputFormat extends DelimitedInputFormat {
	private static final long serialVersionUID = 1L;
	
	private final IntValue integer = new IntValue(1);

	@Override
	public Record readRecord(Record target, byte[] bytes, int offset, int numBytes) {
		target.setField(0, this.integer);
		target.setField(1, this.integer);
		return target;
	}

	@Override
	public FileBaseStatistics getStatistics(BaseStatistics cachedStatistics) {
		return (cachedStatistics instanceof FileBaseStatistics) ? (FileBaseStatistics) cachedStatistics : null;
	}
}
