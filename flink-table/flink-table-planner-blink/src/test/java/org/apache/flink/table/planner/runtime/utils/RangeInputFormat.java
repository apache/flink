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

package org.apache.flink.table.planner.runtime.utils;

import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.table.data.BoxedWrapperRowData;
import org.apache.flink.table.data.RowData;

import java.io.IOException;

/**
 * An input format that returns objects from a range.
 */
public class RangeInputFormat extends GenericInputFormat<RowData> implements NonParallelInput {

	private static final long serialVersionUID = 1L;

	private long start;
	private long end;

	private transient long current;
	private transient BoxedWrapperRowData reuse;

	public RangeInputFormat(long start, long end) {
		this.start = start;
		this.end = end;
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return current >= end;
	}

	@Override
	public void open(GenericInputSplit split) throws IOException {
		super.open(split);
		this.current = start;
	}

	@Override
	public RowData nextRecord(RowData ignore) throws IOException {
		if (reuse == null) {
			reuse = new BoxedWrapperRowData(1);
		}
		reuse.setLong(0, current);
		current++;
		return reuse;
	}
}
