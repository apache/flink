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

import java.util.List;

/**
 * An abstract class for {@link Mapper Mappers} with a model.
 */
public abstract class ModelMapper extends Mapper {

	/**
	 * Field names of the model rows.
	 */
	private final String[] modelFieldNames;

	/**
	 * Field types of the model rows.
	 */
	private final DataType[] modelFieldTypes;

	/**
	 * Constructs a ModelMapper.
	 *
	 * @param modelSchema The schema of the model rows passed to {@link #loadModel(List)}.
	 * @param dataSchema The schema of the input data rows.
	 * @param params The parameters of this ModelMapper.
	 */
	public ModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		this.modelFieldNames = modelSchema.getFieldNames();
		this.modelFieldTypes = modelSchema.getFieldDataTypes();
	}

	/**
	 * Get the schema of the model rows that are passed to {@link #loadModel(List)}.
	 *
	 * @return The schema of the model rows.
	 */
	protected TableSchema getModelSchema() {
		return TableSchema.builder().fields(this.modelFieldNames, this.modelFieldTypes).build();
	}

	/**
	 * Load the model from the list of rows.
	 *
	 * @param modelRows The list of rows that containing the model.
	 */
	public abstract void loadModel(List<Row> modelRows);
}
