/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.streamvertex;

import java.util.ArrayList;

import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.runtime.operators.DataSourceTask;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.io.network.api.RecordWriter;

public class MockRecordWriter extends RecordWriter<SerializationDelegate<StreamRecord<Tuple1<Integer>>>> {

	public ArrayList<Integer> emittedRecords;

	public MockRecordWriter(DataSourceTask<?> inputBase, Class<StreamRecord<Tuple1<Integer>>> outputClass) {
		super(inputBase);
	}

	public boolean initList() {
		emittedRecords = new ArrayList<Integer>();
		return true;
	}
	
	@Override
	public void emit(SerializationDelegate<StreamRecord<Tuple1<Integer>>> record) {
		emittedRecords.add(record.getInstance().getObject().f0);
	}
}
