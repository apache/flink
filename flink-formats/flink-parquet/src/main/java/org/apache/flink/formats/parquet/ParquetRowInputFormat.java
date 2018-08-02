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

import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A subclass of {@link ParquetInputFormat} to read from Parquet files and convert to {@link Row}.
 * It is mainly used to integrate with table API and batch SQL.
 */
public class ParquetRowInputFormat extends ParquetInputFormat<Row> implements ResultTypeQueryable<Row> {
	private static final long MILLISECONDS = 1000L;
	private static final long serialVersionUID = 11L;
	private static final Logger LOG = LoggerFactory.getLogger(ParquetRowInputFormat.class);
	private RowTypeInfo returnType;
	private boolean timeStampRewrite;
	private int tsIndex;

	public ParquetRowInputFormat(Path path, RowTypeInfo rowTypeInfo) {
		super(path, rowTypeInfo.getFieldTypes(), rowTypeInfo.getFieldNames());
		this.returnType = new RowTypeInfo(readType.getFieldTypes().clone(), readType.getFieldNames());
		this.timeStampRewrite = false;
		LOG.debug(String.format("Created ParquetRowInputFormat with path [%s]", path.toString()));
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		return returnType;
	}

	@Override
	protected Row convert(Row row) {
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
			throw new RuntimeException(String.format("Fail to extract timestamp field for row schema: [%s]",
				readType.toString()));
		}

		this.returnType.getFieldTypes()[tsIndex] = SqlTimeTypeInfo.TIMESTAMP;
		this.timeStampRewrite = true;
		LOG.debug("Read parquet record as row type: {}", readType.toString());
	}

	public boolean isTimeStampRewrite() {
		return timeStampRewrite;
	}
}
