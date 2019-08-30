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

package org.apache.flink.ml.common.model;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * This is the abstract base class for saving/loading model data to/from a collection of {@link Row}s.
 *
 * @param <M1> Type of model data that would be saved to a collection of rows.
 * @param <M2> Type of model data that is loaded from a collection of rows.
 */
public interface ModelDataConverter<M1, M2> {
	/**
	 * Save the model data to a collection of rows.
	 *
	 * @param modelData The model data to save.
	 * @param collector A collector for rows.
	 */
	void save(M1 modelData, Collector<Row> collector);

	/**
	 * Load the model data from a collection of rows.
	 *
	 * @param rows The collection of rows from which model data is loaded.
	 * @return The model data.
	 */
	M2 load(List<Row> rows);

	/**
	 * Get the schema of the table to which the model data is saved.
	 *
	 * @return The schema.
	 */
	TableSchema getModelSchema();
}
