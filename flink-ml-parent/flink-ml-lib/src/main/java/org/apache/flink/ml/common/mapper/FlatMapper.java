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
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * Abstract class for flatMappers.
 * FlatMapper maps a row to zero, one or multiple rows.
 */
public abstract class FlatMapper implements Serializable {

	/**
	 * schema of the input.
	 */
	protected TableSchema dataSchema;

	/**
	 * params used for FlatMapper.
	 * User can set the params before that the FlatMapper is executed.
	 */
	protected Params params;

	public FlatMapper(TableSchema dataSchema, Params params) {
		this.dataSchema = dataSchema;
		this.params = (null == params) ? new Params() : params.clone();
	}

	/**
	 * The core method of the FlatMapper.
	 * Takes a row from the input and maps it to multiple rows.
	 *
	 * @param row    The input row.
	 * @param output The collector for returning the result values.
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 *                   to fail.
	 */
	public abstract void flatMap(Row row, Collector<Row> output) throws Exception;

	/**
	 * Get the table schema(includes column names and types) of the calculation result.
	 *
	 * @return the table schema of output Row type data
	 */
	public abstract TableSchema getOutputSchema();

	/**
	 * Generate new instance of given FlatMapper class.
	 *
	 * @param flatMapperClassName Name of the FlatMapper class
	 * @param dataSchema          The schema of input Table type data.
	 * @param params              The parameters for the instance construction.
	 * @return new instance of given FlatMapper class
	 * @throws Exception if flatMapperClass is not the class of {@link FlatMapper}
	 */
	public static FlatMapper of(
		String flatMapperClassName,
		TableSchema dataSchema,
		Params params) throws Exception {

		return of(Class.forName(flatMapperClassName), dataSchema, params);
	}

	/**
	 * Generate new instance of given FlatMapper class.
	 *
	 * @param flatMapperClass FlatMapper class of the new instance
	 * @param dataSchema      The schema of input Table type data.
	 * @param params          the parameters for the instance construction.
	 * @return new instance of given FlatMapper class
	 * @throws Exception if flatMapperClass is not the class of {@link FlatMapper}
	 */
	public static FlatMapper of(
		Class flatMapperClass,
		TableSchema dataSchema,
		Params params) throws Exception {
		
		if (FlatMapper.class.isAssignableFrom(flatMapperClass)) {
			return (FlatMapper) flatMapperClass.getConstructor(TableSchema.class, Params.class)
				.newInstance(dataSchema, params);
		} else {
			throw new IllegalArgumentException(
				"FlatMapper is not assignable from this class : " + flatMapperClass.getCanonicalName());
		}
	}
}
