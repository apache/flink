/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.mapper;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.io.Serializable;

/**
 * Abstract class for mappers. A mapper takes one row as input and transform it into another row.
 */
public abstract class Mapper implements Serializable {

	/**
	 * Schema of the input rows.
	 */
	private final String[] dataFieldNames;
	private final DataType[] dataFieldTypes;

	/**
	 * Parameters for the Mapper.
	 * Users can set the params before the Mapper is executed.
	 */
	protected final Params params;

	/**
	 * Construct a Mapper.
	 *
	 * @param dataSchema The schema of input rows.
	 * @param params The parameters for this mapper.
	 */
	public Mapper(TableSchema dataSchema, Params params) {
		this.dataFieldNames = dataSchema.getFieldNames();
		this.dataFieldTypes = dataSchema.getFieldDataTypes();
		this.params = (null == params) ? new Params() : params.clone();
	}

	/**
	 * Get the schema of input rows.
	 *
	 * @return The schema of input rows.
	 */
	protected TableSchema getDataSchema() {
		return TableSchema.builder().fields(dataFieldNames, dataFieldTypes).build();
	}

	/**
	 * Map a row to a new row.
	 *
	 * @param row The input row.
	 * @return A new row.
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation to fail.
	 */
	public abstract Row map(Row row) throws Exception;

	/**
	 * Get the schema of the output rows of {@link #map(Row)} method.
	 *
	 * @return The table schema of the output rows of {@link #map(Row)} method.
	 */
	public abstract TableSchema getOutputSchema();

}
