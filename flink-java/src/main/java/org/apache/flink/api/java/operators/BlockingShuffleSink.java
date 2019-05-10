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

package org.apache.flink.api.java.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.GenericDataSinkBase;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.BlockingShuffleOutputFormat;

/**
 * This class is used for generate a OutputFormatVertex of placeholder.
 * @param <T>
 */
@Internal
public final class BlockingShuffleSink<T> extends DataSink<T> {

	private final BlockingShuffleOutputFormat<T> intermediateResultOutputFormat;

	public BlockingShuffleSink(DataSet<T> data, BlockingShuffleOutputFormat<T> format, TypeInformation<T> type) {
		super(data, format, type);
		this.intermediateResultOutputFormat = format;
	}

	@Override
	public final GenericDataSinkBase<T> translateToDataFlow(Operator<T> input) {
		GenericDataSinkBase<T> sink = super.translateToDataFlow(input);
		sink.setIntermediateDataSetID(intermediateResultOutputFormat.getIntermediateDataSetId());
		return sink;
	}
}
