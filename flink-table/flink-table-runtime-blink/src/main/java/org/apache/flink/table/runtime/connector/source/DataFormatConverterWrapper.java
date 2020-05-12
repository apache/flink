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

import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.util.DataFormatConverters.DataFormatConverter;

import javax.annotation.Nullable;

/**
 * This class wraps a {@link DataFormatConverter} so it can be used in
 * a {@link DynamicTableSource} as a {@link DynamicTableSource.DataStructureConverter}.
 */
public final class DataFormatConverterWrapper implements DynamicTableSource.DataStructureConverter {

	private static final long serialVersionUID = 1L;

	private final DataFormatConverter<Object, Object> formatConverter;

	public DataFormatConverterWrapper(DataFormatConverter<Object, Object> formatConverter) {
		this.formatConverter = formatConverter;
	}

	@Override
	public void open(RuntimeConverter.Context context) {
		// do nothing
	}

	@Nullable
	@Override
	public Object toInternal(@Nullable Object externalStructure) {
		return formatConverter.toInternal(externalStructure);
	}
}
