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

package org.apache.flink.connector.base.source.reader.mocks;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;

import java.util.ArrayList;

/**
 * A {@code ReaderOutput} for testing that collects the emitted records.
 */
public class TestingReaderOutput<E> implements ReaderOutput<E> {

	private final ArrayList<E> emittedRecords = new ArrayList<>();

	@Override
	public void collect(E record) {
		emittedRecords.add(record);
	}

	@Override
	public void collect(E record, long timestamp) {
		collect(record);
	}

	@Override
	public void emitWatermark(Watermark watermark) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void markIdle() {
		throw new UnsupportedOperationException();
	}

	@Override
	public SourceOutput<E> createOutputForSplit(String splitId) {
		return this;
	}

	@Override
	public void releaseOutputForSplit(String splitId) {}

	// ------------------------------------------------------------------------

	public ArrayList<E> getEmittedRecords() {
		return emittedRecords;
	}

	public void clearEmittedRecords() {
		emittedRecords.clear();
	}
}
