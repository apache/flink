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

package org.apache.flink.formats.parquet;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;


/**
 * A subclass of {@link ParquetInputFormat} to read from Parquet files and convert to {@link Row}.
 * It is mainly used to integrate with table API and` SQL.
 */
public class ParquetRowInputFormat extends ParquetInputFormat<Row> implements ResultTypeQueryable<Row> {
	private static final long serialVersionUID = 11L;
	private static final Logger LOG = LoggerFactory.getLogger(ParquetRowInputFormat.class);
	private boolean timeStampRewrite;
	private RowTypeInfo returnType;
	private int tsIndex;

	public ParquetRowInputFormat(Path path, MessageType messageType) {
		super(path, messageType);
		this.returnType = new RowTypeInfo(getFieldTypes(), getFieldNames());
		this.timeStampRewrite = false;
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		return returnType;
	}

	@Override
	protected Row convert(Row row) {
		if (timeStampRewrite) {
			row.setField(tsIndex, new Timestamp((long) row.getField(tsIndex)));
		}
		return row;
	}

	/**
	 * Convert long or double field in parquet schema to SqlTimeTypeInfo.TIMESTAMP, so that the row return can
	 * be directly used for window aggregation in table API. Rewrite the time stamp field needs to come with
	 * overriding the convert function.

	 * @param fieldName the field needs to change to TIMESTAMP type
	 */
	public void rewriteTimeStampField(String fieldName) {
		this.tsIndex = returnType.getFieldIndex(fieldName);
		if (tsIndex == -1) {
			throw new RuntimeException(String.format("Cannot find field %s in row schema: [%s]",
				returnType.toString()));
		}

		TypeInformation<?> originalType = returnType.getTypeAt(fieldName);
		if (!originalType.equals(BasicTypeInfo.LONG_TYPE_INFO)
			&& !originalType.equals(BasicTypeInfo.DOUBLE_TYPE_INFO)) {
			throw new RuntimeException(String.format("Original type of field %s is not Long or Double, can't convert "
				+ " to java.sql.Timestamp", fieldName));
		}

		this.returnType.getFieldTypes()[tsIndex] = SqlTimeTypeInfo.TIMESTAMP;
		this.timeStampRewrite = true;
		LOG.debug("After rewrite field {} as Timestamp, Read parquet record as row type: {}",
			fieldName, returnType.toString());
	}

	public boolean isTimeStampRewrite() {
		return timeStampRewrite;
	}
}
