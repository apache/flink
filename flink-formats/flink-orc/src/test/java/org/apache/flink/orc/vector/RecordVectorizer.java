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

package org.apache.flink.orc.vector;

import org.apache.flink.orc.data.Record;
import org.apache.flink.orc.util.OrcBulkWriterTestUtil;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

/**
 * A Vectorizer implementation used for tests.
 *
 * <p>It transforms an input element which is of type {@link Record}
 * to a VectorizedRowBatch.
 */
public class RecordVectorizer extends Vectorizer<Record> implements Serializable {

	public RecordVectorizer(String schema) {
		super(schema);
	}

	@Override
	public void vectorize(Record element, VectorizedRowBatch batch) throws IOException {
		BytesColumnVector stringVector = (BytesColumnVector) batch.cols[0];
		LongColumnVector intColVector = (LongColumnVector) batch.cols[1];

		int row = batch.size++;

		stringVector.setVal(row, element.getName().getBytes(StandardCharsets.UTF_8));
		intColVector.vector[row] = element.getAge();

		this.addUserMetadata(OrcBulkWriterTestUtil.USER_METADATA_KEY, OrcBulkWriterTestUtil.USER_METADATA_VALUE);
	}

}
