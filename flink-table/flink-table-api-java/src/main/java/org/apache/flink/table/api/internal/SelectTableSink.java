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

package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.util.Iterator;

/**
 * An internal special {@link TableSink} to collect the select query result to local client.
 */
@Internal
public interface SelectTableSink extends TableSink<Row> {

	default TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		// disable this to make sure there is only one SelectTableSink instance
		// so that the instance can be shared within optimizing and executing in client
		throw new UnsupportedOperationException();
	}

	/**
	 * Set the job client associated with the select job to retrieve the result.
	 */
	void setJobClient(JobClient jobClient);

	/**
	 * Returns the select result as row iterator.
	 */
	Iterator<Row> getResultIterator();
}
