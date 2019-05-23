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

package org.apache.flink.table.sources;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.utils.TableConnectorUtils;

/**
 * Defines an external table with the schema that is provided by {@link TableSource#getTableSchema}.
 *
 * <p>The data of a {@link TableSource} is produced as a {@code DataSet} in case of a {@code BatchTableSource}
 * or as a {@code DataStream} in case of a {@code StreamTableSource}. The type of ths produced
 * {@code DataSet} or {@code DataStream} is specified by the {@link TableSource#getReturnType} method.
 *
 * <p>By default, the fields of the {@link TableSchema} are implicitly mapped by name to the fields of
 * the return type {@link TypeInformation}. An explicit mapping can be defined by implementing the
 * {@link DefinedFieldMapping} interface.
 *
 * @param <T> The return type of the {@link TableSource}.
 */
@PublicEvolving
public interface TableSource<T> {

	/**
	 * Returns the {@link TypeInformation} for the return type of the {@link TableSource}.
	 * The fields of the return type are mapped to the table schema based on their name.
	 *
	 * @return The type of the returned {@code DataSet} or {@code DataStream}.
	 */
	TypeInformation<T> getReturnType();

	/**
	 * Returns the schema of the produced table.
	 *
	 * @return The {@link TableSchema} of the produced table.
	 */
	TableSchema getTableSchema();

	/**
	 * Describes the table source.
	 *
	 * @return A String explaining the {@link TableSource}.
	 */
	default String explainSource() {
		return TableConnectorUtils.generateRuntimeName(getClass(), getTableSchema().getFieldNames());
	}
}
