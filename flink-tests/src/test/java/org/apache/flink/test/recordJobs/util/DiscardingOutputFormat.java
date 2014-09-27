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


package org.apache.flink.test.recordJobs.util;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Record;

/**
 * A simple output format that discards all data by doing nothing.
 */
public class DiscardingOutputFormat implements OutputFormat<Record> {
	
	private static final long serialVersionUID = 1L;

	@Override
	public void configure(Configuration parameters) {}


	@Override
	public void open(int taskNumber, int numTasks) {}

	@Override
	public void writeRecord(Record record) {}


	@Override
	public void close() {}
}
