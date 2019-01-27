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

package org.apache.flink.table.sources.orc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.TypeConverters;
import org.apache.flink.table.dataformat.ColumnarRow;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Arrays;

/**
 * A subclass of {@link OrcInputFormat} to read from
 * Orc files and convert to {@link ColumnarRow}.
 */
public class VectorizedColumnRowInputOrcFormat extends OrcInputFormat<ColumnarRow, ColumnarRow>
		implements ResultTypeQueryable<ColumnarRow> {

	private static final long serialVersionUID = -857535186065774140L;

	private boolean copyToFlink;

	public VectorizedColumnRowInputOrcFormat(Path filePath, InternalType[] fieldTypes, String[] fieldNames) {
		this(filePath, fieldTypes, fieldNames, false);
	}

	public VectorizedColumnRowInputOrcFormat(Path filePath, InternalType[] fieldTypes, String[] fieldNames, boolean copyToFlink) {
		super(filePath, fieldTypes, fieldNames);
		this.copyToFlink = copyToFlink;
	}

	@Override
	protected ColumnarRow convert(ColumnarRow current) {
		return current;
	}

	@Override
	protected RecordReader createReader(FileInputSplit fileSplit, TaskAttemptContext taskAttemptContext) throws IOException {
		return new OrcVectorizedColumnRowReader(fieldTypes, fieldNames, schemaFieldNames, copyToFlink);
	}

	@Override
	public TypeInformation<ColumnarRow> getProducedType() {
		TypeInformation[] typeInfos =
			Arrays.stream(this.fieldTypes)
				.map(TypeConverters::createExternalTypeInfoFromDataType)
				.toArray(TypeInformation[]::new);
		return (TypeInformation) new BaseRowTypeInfo(typeInfos);
	}
}
