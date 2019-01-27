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

package org.apache.flink.table.api;

/**
 * watermark.
 */
public class Watermark {

	private final String name;

	private final String eventTime;

	private final long offset;

	public Watermark(String name, String eventTime, long offset) {
		this.name = name;
		this.eventTime = eventTime;
		this.offset = offset;
	}

	public String name() {
		return this.name;
	}

	public String eventTime() {
		return this.eventTime;
	}

	public long offset() {
		return this.offset;
	}
}
