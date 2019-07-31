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

/**
 * Abstract class for mappers with model.
 * ModelMapper transform one Row type data into one Row type result data.
 * Operations that produce zero, one or more Row type result data per Row type data
 * can also use the {@link FlatModelMapper}.
 */
public abstract class ModelMapper extends FlatModelMapper implements MapOpInterface {

	public ModelMapper(TableSchema modelScheme, TableSchema dataSchema, Params params) {
		super(modelScheme, dataSchema, params);
	}

	/**
	 * Wrapper method for the {@link FlatModelMapper}.
	 * @param row the input row.
	 * @param output the output collector
	 * @throws Exception if {@link Collector#collect(Object)} throws exception.
	 */
	@Override
	public void flatMap(Row row, Collector<Row> output) throws Exception {
		output.collect(map(row));
	}

}
