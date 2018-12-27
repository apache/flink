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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.Types;

/**
 * Defines an external {@link TableSink} to emit a streaming {@link Table} with insert, update, and
 * delete changes. The {@link Table} must be have unique key fields (atomic or composite) or be
 * append-only.
 *
 * <p>If the {@link Table} does not have a unique key and is not append-only, a
 * {@link TableException} will be thrown.
 *
 * <p>The unique key of the table is configured by the {@code UpsertStreamTableSink#setKeyFields()}
 * method.
 *
 * <p>The {@link Table} will be converted into a stream of upsert and delete messages which are
 * encoded as {@link Tuple2}. The first field is a {@link Boolean} flag to indicate the message
 * type. The second field holds the record of the requested type T.
 *
 * <p>A message with true {@link Boolean} field is an upsert message for the configured key.
 * A message with false flag is a delete message for the configured key.
 *
 * <p>If the table is append-only, all messages will have a true flag and must be interpreted
 * as insertions.
 *
 * @param <T> Type of records that this {@link TableSink} expects and supports.
 */
public interface UpsertStreamTableSink<T> extends StreamTableSink<Tuple2<Boolean, T>> {

	/**
	 * Configures the unique key fields of the {@link Table} to write.
	 * The method is called after {@code TableSink.configure()}.
	 *
	 * <p>The keys array might be empty, if the table consists of a single (updated) record.
	 * If the table does not have a key and is append-only, the keys attribute is null.
	 *
	 * @param keys the field names of the table's keys, an empty array if the table has a single
	 *             row, and null if the table is append-only and has no key.
	 */
	void setKeyFields(String[] keys);

	/**
	 * Enforce output key fields. Users can use this method to enforce output key fields which
	 * may be different from key fields of the query. Note: use it when you know what you are doing.
	 */
	default String[] enforceKeyFields() {
		return new String[]{};
	}

	/**
	 * Specifies whether the {@link Table} to write is append-only or not.
	 *
	 * @param isAppendOnly true if the table is append-only, false otherwise.
	 */
	void setIsAppendOnly(Boolean isAppendOnly);

	/** Returns the requested record type. */
	TypeInformation<T> getRecordType();

	/** Emits the DataStream. */
	void emitDataStream(DataStream<Tuple2<Boolean, T>> dataStream);

	@Override
	default TypeInformation<Tuple2<Boolean, T>> getOutputType() {
		return new TupleTypeInfo(Types.BOOLEAN(), getRecordType());
	}
}
