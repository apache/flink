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

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.annotation.Public;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * The base record of {@link SourceFunctionV2}.
 *
 * @param <T> the type parameter
 */
@Public
public class SourceRecord<T> {

	private T record;

	private long timestamp = -1;

	private Watermark watermark;

	/**
	 * Instantiates a new Source record.
	 */
	public SourceRecord() {

	}

	/**
	 * Reset source record.
	 *
	 * @return the source record
	 */
	public SourceRecord<T> reset() {
		this.record = null;
		this.timestamp = -1;
		this.watermark = null;

		return this;
	}

	/**
	 * Gets record.
	 *
	 * @return the record
	 */
	public T getRecord() {
		return record;
	}

	/**
	 * Sets record.
	 *
	 * @param record the record
	 */
	public void setRecord(T record) {
		this.record = record;
	}

	/**
	 * Gets timestamp.
	 *
	 * @return the timestamp
	 */
	public long getTimestamp() {
		return timestamp;
	}

	/**
	 * Sets timestamp.
	 *
	 * @param timestamp the timestamp
	 */
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	/**
	 * Gets watermark.
	 *
	 * @return the watermark
	 */
	public Watermark getWatermark() {
		return watermark;
	}

	/**
	 * Sets watermark.
	 *
	 * @param watermark the watermark
	 */
	public void setWatermark(Watermark watermark) {
		this.watermark = watermark;
	}

	/**
	 * Replace source record with user record.
	 *
	 * @param record the record
	 * @return the source record
	 */
	public SourceRecord<T> replace(T record) {
		reset();
		setRecord(record);
		return this;
	}

	/**
	 * Replace source record with user record and timestamp.
	 *
	 * @param record    the record
	 * @param timestamp the timestamp
	 * @return the source record
	 */
	public SourceRecord<T> replace(T record, long timestamp) {
		reset();
		setRecord(record);
		setTimestamp(timestamp);
		return this;
	}

	/**
	 * Replace source record with watermark.
	 *
	 * @param watermark the watermark
	 * @return the source record
	 */
	public SourceRecord<T> replace(Watermark watermark) {
		reset();
		setWatermark(watermark);
		return this;
	}

	/**
	 * Create source record with user record.
	 *
	 * @param <T>    the type parameter
	 * @param record the record
	 * @return the source record
	 */
	public static <T> SourceRecord<T> create(T record) {
		SourceRecord<T> sourceRecord = new SourceRecord<>();
		sourceRecord.setRecord(record);
		return sourceRecord;
	}

	/**
	 * Create source record with user record and timestamp.
	 *
	 * @param <T>       the type parameter
	 * @param record    the record
	 * @param timestamp the timestamp
	 * @return the source record
	 */
	public static <T> SourceRecord<T> create(T record, long timestamp) {
		SourceRecord<T> sourceRecord = new SourceRecord<>();
		sourceRecord.setRecord(record);
		sourceRecord.setTimestamp(timestamp);
		return sourceRecord;
	}

	/**
	 * Create source record with watermark.
	 *
	 * @param <T>       the type parameter
	 * @param watermark the watermark
	 * @return the source record
	 */
	public static <T> SourceRecord<T> create(Watermark watermark) {
		SourceRecord<T> sourceRecord = new SourceRecord<>();
		sourceRecord.setWatermark(watermark);
		return sourceRecord;
	}
}
