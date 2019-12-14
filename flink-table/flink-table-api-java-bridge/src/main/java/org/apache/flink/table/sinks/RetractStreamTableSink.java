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

package org.apache.flink.table.sinks;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.api.Table;

/**
 * Defines an external {@link TableSink} to emit a streaming {@link Table} with insert, update,
 * and delete changes.
 *
 * <p>The table will be converted into a stream of accumulate and retraction messages which are
 * encoded as {@link Tuple2}. The first field is a {@link Boolean} flag to indicate the message type.
 * The second field holds the record of the requested type {@link T}.
 *
 * <p>A message with true {@link Boolean} flag is an accumulate (or add) message.
 *
 * <p>A message with false flag is a retract message.
 *
 * @param <T> Type of records that this {@link TableSink} expects and supports.
 */
@PublicEvolving
public interface RetractStreamTableSink<T> extends StreamTableSink<Tuple2<Boolean, T>> {

	/**
	 * Returns the requested record type.
	 */
	TypeInformation<T> getRecordType();

	@Override
	default TypeInformation<Tuple2<Boolean, T>> getOutputType() {
		return new TupleTypeInfo<>(Types.BOOLEAN, getRecordType());
	}
}
