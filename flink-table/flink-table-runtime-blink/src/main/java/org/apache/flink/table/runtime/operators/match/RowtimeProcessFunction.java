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

package org.apache.flink.table.runtime.operators.match;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.util.Collector;

/**
 * ProcessFunction to copy a timestamp from a {@link BaseRow} field into the
 * {@link org.apache.flink.streaming.runtime.streamrecord.StreamRecord}.
 */
public class RowtimeProcessFunction
	extends ProcessFunction<BaseRow, BaseRow> implements ResultTypeQueryable<BaseRow> {

	private static final long serialVersionUID = 1L;

	private final int rowtimeIdx;
	private transient TypeInformation<BaseRow> returnType;

	public RowtimeProcessFunction(int rowtimeIdx, TypeInformation<BaseRow> returnType) {
		this.rowtimeIdx = rowtimeIdx;
		this.returnType = returnType;
	}

	@Override
	public void processElement(BaseRow value, Context ctx, Collector<BaseRow> out) throws Exception {
		long timestamp = value.getLong(rowtimeIdx);
		((TimestampedCollector<BaseRow>) out).setAbsoluteTimestamp(timestamp);
		out.collect(value);
	}

	@Override
	public TypeInformation<BaseRow> getProducedType() {
		return returnType;
	}
}
