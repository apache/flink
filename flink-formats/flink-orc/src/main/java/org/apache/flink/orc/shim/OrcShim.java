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

package org.apache.flink.orc.shim;

import org.apache.flink.orc.OrcSplitReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * A shim layer to support orc with different dependents versions of Hive.
 */
public interface OrcShim extends Serializable {

	/**
	 * Create orc {@link RecordReader} from conf, schema and etc...
	 */
	RecordReader createRecordReader(
			Configuration conf,
			TypeDescription schema,
			int[] selectedFields,
			List<OrcSplitReader.Predicate> conjunctPredicates,
			org.apache.flink.core.fs.Path path,
			long splitStart,
			long splitLength) throws IOException;

	/**
	 * Read the next row batch.
	 */
	boolean nextBatch(RecordReader reader, VectorizedRowBatch rowBatch) throws IOException;

	/**
	 * Default with orc dependent, we should use v2.3.0.
	 */
	static OrcShim defaultShim() {
		return new OrcShimV230();
	}

	/**
	 * Create shim from hive version.
	 */
	static OrcShim createShim(String hiveVersion) {
		if (hiveVersion.startsWith("2.0")) {
			return new OrcShimV200();
		} else if (hiveVersion.startsWith("2.1")) {
			return new OrcShimV210();
		} else if (hiveVersion.startsWith("2.2") ||
				hiveVersion.startsWith("2.3") ||
				hiveVersion.startsWith("3.")) {
			return new OrcShimV230();
		} else {
			throw new UnsupportedOperationException(
					"Unsupported hive version for orc shim: " + hiveVersion);
		}
	}
}
