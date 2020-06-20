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

package org.apache.flink.table.runtime.operators.window.assigners;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.CountWindow;
import org.apache.flink.table.runtime.operators.window.internal.InternalWindowProcessFunction;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * A {@link WindowAssigner} that windows elements into fixed-size windows
 * based on the count number of the elements. Windows cannot overlap.
 */
public class CountTumblingWindowAssigner extends WindowAssigner<CountWindow> {
	private static final long serialVersionUID = -3857633557257357800L;

	private final long size;

	private transient ValueState<Long> count;

	private CountTumblingWindowAssigner(long size) {
		this.size = size;
	}

	@Override
	public void open(InternalWindowProcessFunction.Context<?, CountWindow> ctx) throws Exception {
		String descriptorName = "tumble-count-assigner";
		ValueStateDescriptor<Long> countDescriptor = new ValueStateDescriptor<>(
			descriptorName,
			Types.LONG);
		this.count = ctx.getPartitionedState(countDescriptor);
	}

	@Override
	public Collection<CountWindow> assignWindows(RowData element, long timestamp) throws IOException {
		Long countValue = count.value();
		long currentCount = countValue == null ? 0L : countValue;
		long id = currentCount / size;
		count.update(currentCount + 1);
		return Collections.singleton(new CountWindow(id));
	}

	@Override
	public TypeSerializer<CountWindow> getWindowSerializer(ExecutionConfig executionConfig) {
		return new CountWindow.Serializer();
	}

	@Override
	public boolean isEventTime() {
		return false;
	}

	@Override
	public String toString() {
		return "CountTumblingWindow(" + size + ")";
	}

	public static CountTumblingWindowAssigner of(long size) {
		return new CountTumblingWindowAssigner(size);
	}
}
