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


package org.apache.flink.hadoopcompatibility.mapred.record.datatypes;

import org.apache.flink.types.Record;
import org.apache.flink.types.Value;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class WritableWrapperConverter<K extends WritableComparable, V extends Writable> implements HadoopTypeConverter<K,V> {
	private static final long serialVersionUID = 1L;

	@Override
	public void convert(Record flinkRecord, K hadoopKey, V hadoopValue) {
		flinkRecord.setField(0, convertKey(hadoopKey));
		flinkRecord.setField(1, convertValue(hadoopValue));
	}
	
	@SuppressWarnings("unchecked")
	private final Value convertKey(K in) {
		return new WritableComparableWrapper(in);
	}
	
	private final Value convertValue(V in) {
		return new WritableWrapper<V>(in);
	}
}
