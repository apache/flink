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

package org.apache.flink.orc.nohive.shim;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.orc.OrcFilters;
import org.apache.flink.orc.nohive.vector.OrcNoHiveBatchWrapper;
import org.apache.flink.orc.shim.OrcShim;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.orc.shim.OrcShimV200.computeProjectionMask;
import static org.apache.flink.orc.shim.OrcShimV200.getOffsetAndLengthForSplit;

/**
 * Shim for orc reader without hive dependents.
 */
public class OrcNoHiveShim implements OrcShim<VectorizedRowBatch> {

	private static final long serialVersionUID = 1L;

	@Override
	public RecordReader createRecordReader(
			Configuration conf,
			TypeDescription schema,
			int[] selectedFields,
			List<OrcFilters.Predicate> conjunctPredicates,
			org.apache.flink.core.fs.Path path,
			long splitStart,
			long splitLength) throws IOException {
		// open ORC file and create reader
		org.apache.hadoop.fs.Path hPath = new org.apache.hadoop.fs.Path(path.toUri());

		Reader orcReader = OrcFile.createReader(hPath, OrcFile.readerOptions(conf));

		// get offset and length for the stripes that start in the split
		Tuple2<Long, Long> offsetAndLength = getOffsetAndLengthForSplit(
				splitStart, splitLength, orcReader.getStripes());

		// create ORC row reader configuration
		Reader.Options options = new Reader.Options()
				.schema(schema)
				.range(offsetAndLength.f0, offsetAndLength.f1)
				.useZeroCopy(OrcConf.USE_ZEROCOPY.getBoolean(conf))
				.skipCorruptRecords(OrcConf.SKIP_CORRUPT_DATA.getBoolean(conf))
				.tolerateMissingSchema(OrcConf.TOLERATE_MISSING_SCHEMA.getBoolean(conf));

		// TODO configure filters

		// configure selected fields
		options.include(computeProjectionMask(schema, selectedFields));

		// create ORC row reader
		RecordReader orcRowsReader = orcReader.rows(options);

		// assign ids
		schema.getId();

		return orcRowsReader;
	}

	@Override
	public OrcNoHiveBatchWrapper createBatchWrapper(TypeDescription schema, int batchSize) {
		return new OrcNoHiveBatchWrapper(schema.createRowBatch(batchSize));
	}

	@Override
	public boolean nextBatch(RecordReader reader, VectorizedRowBatch rowBatch) throws IOException {
		return reader.nextBatch(rowBatch);
	}
}
