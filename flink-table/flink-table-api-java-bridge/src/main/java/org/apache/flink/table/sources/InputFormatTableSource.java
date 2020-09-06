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

package org.apache.flink.table.sources;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo;

/**
 * Defines an external bounded table and provides access to its data.
 *
 * @param <T> Type of the bounded {@link InputFormat} created by this {@link TableSource}.
 */
@Experimental
public abstract class InputFormatTableSource<T> implements StreamTableSource<T> {

	/**
	 * Returns an {@link InputFormat} for reading the data of the table.
	 */
	public abstract InputFormat<T, ?> getInputFormat();

	/**
	 * Always returns true which indicates this is a bounded source.
	 */
	@Override
	public final boolean isBounded() {
		return true;
	}

	@SuppressWarnings("unchecked")
	@Override
	public final DataStream<T> getDataStream(StreamExecutionEnvironment execEnv) {
		TypeInformation<T> typeInfo = (TypeInformation<T>) fromDataTypeToLegacyInfo(getProducedDataType());
		return execEnv.createInput(getInputFormat(), typeInfo);
	}
}
