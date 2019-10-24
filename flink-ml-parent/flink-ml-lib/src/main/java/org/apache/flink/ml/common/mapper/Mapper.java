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
 * Abstract class for mappers.
 */
public abstract class Mapper implements Serializable {

	/**
	 * schema of the input.
	 */
	private final String[] dataFieldNames;
	private final DataType[] dataFieldTypes;

	/**
	 * params used for Mapper.
	 * User can set the params before that the Mapper is executed.
	 */
	protected Params params;

	public Mapper(TableSchema dataSchema, Params params) {
		this.dataFieldNames = dataSchema.getFieldNames();
		this.dataFieldTypes = dataSchema.getFieldDataTypes();
		this.params = (null == params) ? new Params() : params.clone();
	}

	protected TableSchema getDataSchema() {
		return TableSchema.builder().fields(dataFieldNames, dataFieldTypes).build();
	}

	/**
	 * map operation method that maps a row to a new row.
	 *
	 * @param row the input Row type data
	 * @return one Row type data
	 * @throws Exception This method may throw exceptions. Throwing
	 *                   an exception will cause the operation to fail.
	 */
	public abstract Row map(Row row) throws Exception;

	/**
	 * Get the table schema(includes column names and types) of the calculation result.
	 *
	 * @return the table schema of output Row type data
	 */
	public abstract TableSchema getOutputSchema();

}
