/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * The interface provided by Flink task to the {@link SourceReader} to emit records
 * to downstream operators for message processing.
 */
@Public
public interface SourceOutput<T> extends WatermarkOutput {

	/**
	 * Emit a record without a timestamp. Equivalent to {@link #collect(Object, long) collect(timestamp, null)};
	 *
	 * @param record the record to emit.
	 */
	void collect(T record) throws Exception;

	/**
	 * Emit a record with timestamp.
	 *
	 * @param record the record to emit.
	 * @param timestamp the timestamp of the record.
	 */
	void collect(T record, long timestamp) throws Exception;
}
