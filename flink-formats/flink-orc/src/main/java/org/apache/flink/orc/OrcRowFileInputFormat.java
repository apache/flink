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

package org.apache.flink.orc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.util.ArrayResultIterator;
import org.apache.flink.connector.file.src.util.Pool;
import org.apache.flink.types.Row;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;

import java.util.List;

/**
 * An ORC file input format that reads an ORC file and produces a stream of {@link Row} records.
 */
public class OrcRowFileInputFormat extends AbstractOrcFileInputFormat<Row> {

	private static final long serialVersionUID = 1L;

	protected OrcRowFileInputFormat(
			final Configuration hadoopConfig,
			final TypeDescription schema,
			final int[] selectedFields,
			final List<OrcSplitReader.Predicate> conjunctPredicates,
			final int batchSize) {
		super(hadoopConfig, schema, selectedFields, conjunctPredicates, batchSize);
	}

	@Override
	public OrcReaderBatch<Row> createReaderBatch(
			final VectorizedRowBatch orcVectorizedRowBatch,
			final Pool.Recycler<OrcReaderBatch<Row>> recycler,
			final int batchSize) {

		final int[] selectedFields = this.selectedFields;
		final Row[] rows = new Row[batchSize];

		for (int i = 0; i < batchSize; i++) {
			rows[i] = new Row(selectedFields.length);
		}
		return new RowReaderBatch(schema, selectedFields, rows, orcVectorizedRowBatch, recycler);
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		// TODO
		return null;
	}

	// ------------------------------------------------------------------------

	/**
	 * One batch of ORC columnar vectors and Flink Rows, plus the conversion between them.
	 */
	private static final class RowReaderBatch extends OrcReaderBatch<Row> {

		private final TypeDescription schema;

		private final int[] selectedFields;

		private final Row[] rows;

		private final ArrayResultIterator<Row> result;

		RowReaderBatch(
				final TypeDescription schema,
				final int[] selectedFields,
				final Row[] rows,
				final VectorizedRowBatch orcVectorizedRowBatch,
				final Pool.Recycler<OrcReaderBatch<Row>> recycler) {

			super(orcVectorizedRowBatch, recycler);
			this.schema = schema;
			this.selectedFields = selectedFields;
			this.rows = rows;
			this.result = new ArrayResultIterator<>(this::recycle);
		}

		@Override
		public RecordIterator<Row> convertAndGetIterator(
				final VectorizedRowBatch orcVectorizedRowBatch,
				final long startingOffset) {

			OrcBatchReader.fillRows(rows, schema, orcVectorizedRowBatch, selectedFields);
			result.set(rows, orcVectorizedRowBatch.size, startingOffset, 0L);
			return result;
		}
	}
}
