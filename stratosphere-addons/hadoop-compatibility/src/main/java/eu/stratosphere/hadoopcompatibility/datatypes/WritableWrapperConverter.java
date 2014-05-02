/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.hadoopcompatibility.datatypes;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import eu.stratosphere.types.Record;
import eu.stratosphere.types.Value;

public class WritableWrapperConverter<K extends WritableComparable, V extends Writable> implements HadoopTypeConverter<K,V> {
	private static final long serialVersionUID = 1L;

	@Override
	public void convert(Record stratosphereRecord, K hadoopKey, V hadoopValue) {
		stratosphereRecord.setField(0, convertKey(hadoopKey));
		stratosphereRecord.setField(1, convertValue(hadoopValue));
	}
	
	private final Value convertKey(K in) {
		return new WritableComparableWrapper<K>(in);
	}
	
	private final Value convertValue(V in) {
		return new WritableWrapper<V>(in);
	}
}
