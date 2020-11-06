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
import org.apache.flink.connector.file.src.util.Pool;
import org.apache.flink.connector.file.src.util.SingletonResultIterator;
import org.apache.flink.table.data.vector.VectorizedColumnBatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;

import java.util.List;

/**
 * An ORC reader that produces a stream of {@link VectorizedColumnBatch} records.
 * Used in vectorized query processing.
 */
public class OrcVectorizedColumnarFileInputFormat extends AbstractOrcFileInputFormat<VectorizedColumnBatch> {

	private static final long serialVersionUID = 1L;

	protected OrcVectorizedColumnarFileInputFormat(
			final Configuration hadoopConfig,
			final TypeDescription schema,
			final int[] selectedFields,
			final List<OrcSplitReader.Predicate> conjunctPredicates,
			final int batchSize) {
		super(hadoopConfig, schema, selectedFields, conjunctPredicates, batchSize);
	}

	@Override
	public OrcReaderBatch<VectorizedColumnBatch> createReaderBatch(
			final VectorizedRowBatch orcVectorizedRowBatch,
			final Pool.Recycler<OrcReaderBatch<VectorizedColumnBatch>> recycler,
			final int batchSize) {

		final VectorizedColumnBatch flinkColumnBatch = createFlinkColumnBatchFromOrcColumnBatch(orcVectorizedRowBatch);
		return new VectorizedColumnReaderBatch(orcVectorizedRowBatch, flinkColumnBatch, recycler);
	}

	@Override
	public TypeInformation<VectorizedColumnBatch> getProducedType() {
		// TODO
		return null;
	}

	private VectorizedColumnBatch createFlinkColumnBatchFromOrcColumnBatch(VectorizedRowBatch orcVectorizedRowBatch) {
		// TODO
		return null;
	}

	// ------------------------------------------------------------------------

	/**
	 * One batch of ORC columnar vectors and Flink column vectors.
	 */
	private static final class VectorizedColumnReaderBatch extends AbstractOrcFileInputFormat.OrcReaderBatch<VectorizedColumnBatch> {

		private final VectorizedColumnBatch flinkColumnBatch;

		private final SingletonResultIterator<VectorizedColumnBatch> result;

		VectorizedColumnReaderBatch(
				final VectorizedRowBatch orcVectorizedRowBatch,
				final VectorizedColumnBatch flinkColumnBatch,
				final Pool.Recycler<AbstractOrcFileInputFormat.OrcReaderBatch<VectorizedColumnBatch>> recycler) {

			super(orcVectorizedRowBatch, recycler);
			this.flinkColumnBatch = flinkColumnBatch;
			this.result = new SingletonResultIterator<>(this::recycle);
		}

		@Override
		public RecordIterator<VectorizedColumnBatch> convertAndGetIterator(
				final VectorizedRowBatch orcVectorizedRowBatch,
				final long startingOffset) {
			// no copying from the ORC column vectors to the Flink columns vectors necessary,
			// because they point to the same data arrays internally design
			final int numRowsInBatch = orcVectorizedRowBatch.size;
			flinkColumnBatch.setNumRows(numRowsInBatch);
			result.set(flinkColumnBatch, startingOffset, 1);
			return result;
		}
	}
}
