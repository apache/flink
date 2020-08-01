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

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.common.model.ModelSource;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * A class that adapts a {@link ModelMapper} to a Flink {@link RichMapFunction} so the model can be
 * loaded in a Flink job.
 *
 * <p>This adapter class hold the target {@link ModelMapper} and it's {@link ModelSource}. Upon open(),
 * it will load model rows from {@link ModelSource} into {@link ModelMapper}.
 */
public class ModelMapperAdapter extends RichMapFunction<Row, Row> {

	private final ModelMapper mapper;
	private final ModelSource modelSource;

	/**
	 * Construct a ModelMapperAdapter with the given ModelMapper and ModelSource.
	 *
	 * @param mapper The {@link ModelMapper} to adapt.
	 * @param modelSource The {@link ModelSource} to load the model from.
	 */
	public ModelMapperAdapter(ModelMapper mapper, ModelSource modelSource) {
		this.mapper = mapper;
		this.modelSource = modelSource;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		List<Row> modelRows = this.modelSource.getModelRows(getRuntimeContext());
		this.mapper.loadModel(modelRows);
	}

	@Override
	public Row map(Row row) throws Exception {
		return this.mapper.map(row);
	}
}
