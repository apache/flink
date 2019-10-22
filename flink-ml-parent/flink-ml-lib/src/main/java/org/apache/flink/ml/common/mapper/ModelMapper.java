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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.List;

/**
 * Abstract class for mappers with model.
 */
public abstract class ModelMapper extends Mapper {

	/**
	 * Specify where to load model data.
	 */
	private enum ModelSourceType implements Serializable {
		/**
		 * Load model data from flink's broadcast dataset.
		 */
		BroadcastDataset,

		/**
		 * Load model data from data rows.
		 */
		Rows
	}

	private ModelSourceType modelSourceType;

	/**
	 * Load model from broadcast dataset of this name if modelSource == BroadcastDataset.
	 */
	private String modelBroadcastName;

	/**
	 * Load model from these rows if modelSource == Rows.
	 */
	private List<Row> modelRows;


	/**
	 * Field names of the model.
	 */
	private final String[] modelFieldNames;

	/**
	 * Field types of the model.
	 */
	private final DataType[] modelFieldTypes;

	public ModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		this.modelFieldNames = modelSchema.getFieldNames();
		this.modelFieldTypes = modelSchema.getFieldDataTypes();
	}

	protected TableSchema getModelSchema() {
		return TableSchema.builder().fields(this.modelFieldNames, this.modelFieldTypes).build();
	}

	public final void setModelBroadcastName(String modelBroadcastName) {
		Preconditions.checkState(this.modelSourceType == null,
				"Model source could be set only once");
		this.modelBroadcastName = modelBroadcastName;
		this.modelSourceType = ModelSourceType.BroadcastDataset;
	}

	public final void setModelRows(List<Row> rows) {
		Preconditions.checkState(this.modelSourceType == null,
				"Model source could be set only once");
		this.modelRows = rows;
		this.modelSourceType = ModelSourceType.Rows;
	}

	/**
	 * Load model from the list of Row type data.
	 *
	 * @param modelRows the list of Row type data
	 */
	public abstract void loadModel(List<Row> modelRows);

	@Override
	public final void open(Configuration parameters) {
		List<Row> rows;
		switch (this.modelSourceType) {
			case BroadcastDataset:
				rows = getRuntimeContext().getBroadcastVariable(this.modelBroadcastName);
				break;
			case Rows:
				rows = this.modelRows;
				break;
			default:
				throw new IllegalStateException();
		}
		if (rows != null) {
			loadModel(rows);
		}
	}
}
