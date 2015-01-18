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

package org.apache.flink.streaming.api.collector;

import java.io.IOException;
import java.util.List;

import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.io.StreamRecordWriter;
import org.apache.flink.util.Collector;

public class StreamOutput<OUT> implements Collector<SerializationDelegate<StreamRecord<OUT>>> {

	private RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output;

	private List<String> selectedNames;
	private boolean selectAll = true;

	public StreamOutput(RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output,
			List<String> selectedNames) {

		this.output = output;

		if (selectedNames != null) {
			this.selectedNames = selectedNames;
			selectAll = false;
		}
	}

	public void collect(SerializationDelegate<StreamRecord<OUT>> record) {
		try {
			output.emit(record);
		} catch (Exception e) {
			throw new RuntimeException("Could not emit record: " + record.getInstance());
		}
	}

	@Override
	public void close() {
		if (output instanceof StreamRecordWriter) {
			((StreamRecordWriter<SerializationDelegate<StreamRecord<OUT>>>) output).close();
		} else {
			try {
				output.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public boolean isSelectAll() {
		return selectAll;
	}

	public List<String> getSelectedNames() {
		return selectedNames;
	}

	public RecordWriter<SerializationDelegate<StreamRecord<OUT>>> getRecordWriter() {
		return output;
	}

}
