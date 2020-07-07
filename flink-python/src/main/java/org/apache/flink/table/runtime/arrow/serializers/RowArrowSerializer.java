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

package org.apache.flink.table.runtime.arrow.serializers;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.runtime.arrow.ArrowReader;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.runtime.arrow.ArrowWriter;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * It takes {@link Row} as the input type.
 */
@Internal
public class RowArrowSerializer extends ArrowSerializer<Row> {
	public RowArrowSerializer(
		RowType inputType,
		RowType outputType) {
		super(inputType, outputType);
	}

	@Override
	public ArrowWriter<Row> createArrowWriter() {
		return ArrowUtils.createRowArrowWriter(rootWriter, inputType);
	}

	@Override
	public ArrowReader<Row> createArrowReader(VectorSchemaRoot root) {
		return ArrowUtils.createRowArrowReader(root, outputType);
	}
}
