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

package org.apache.flink.table.runtime.connector.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.conversion.DataStructureConverter;

import javax.annotation.Nullable;

/**
 * Implementation of {@link DynamicTableSource.DataStructureConverter}.
 *
 * <p>It wraps the internal {@link DataStructureConverter}.
 */
@Internal
class DataStructureConverterWrapper implements DynamicTableSource.DataStructureConverter {

	private static final long serialVersionUID = 1L;

	private final DataStructureConverter<Object, Object> structureConverter;

	DataStructureConverterWrapper(DataStructureConverter<Object, Object> structureConverter) {
		this.structureConverter = structureConverter;
	}

	@Override
	public void open(RuntimeConverter.Context context) {
		structureConverter.open(context.getClassLoader());
	}

	@Override
	public @Nullable Object toInternal(@Nullable Object externalStructure) {
		return structureConverter.toInternalOrNull(externalStructure);
	}
}
