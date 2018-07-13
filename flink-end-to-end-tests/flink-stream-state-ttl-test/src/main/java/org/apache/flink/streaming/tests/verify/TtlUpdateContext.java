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

package org.apache.flink.streaming.tests.verify;

import javax.annotation.Nonnull;

import java.io.Serializable;

/** Contains relevant for state TTL update data. */
public class TtlUpdateContext<UV, GV> implements Serializable {
	private final int key;

	@Nonnull
	private final String verifierId;

	private final GV valueBeforeUpdate;
	private final UV update;
	private final GV updatedValue;

	private final long timestamp;

	public TtlUpdateContext(int key, @Nonnull String verifierId, GV valueBeforeUpdate, UV update, GV updatedValue) {
		this(key, verifierId, valueBeforeUpdate, update, updatedValue, System.currentTimeMillis());
	}

	private TtlUpdateContext(
		int key, @Nonnull String verifierId, GV valueBeforeUpdate, UV update, GV updatedValue, long timestamp) {
		this.key = key;
		this.verifierId = verifierId;
		this.valueBeforeUpdate = valueBeforeUpdate;
		this.update = update;
		this.updatedValue = updatedValue;
		this.timestamp = timestamp;
	}

	public int getKey() {
		return key;
	}

	@Nonnull
	public String getVerifierId() {
		return verifierId;
	}

	GV getValueBeforeUpdate() {
		return valueBeforeUpdate;
	}

	@Nonnull
	public TtlValue<UV> getUpdateWithTs() {
		return new TtlValue<>(update, timestamp);
	}

	GV getUpdatedValue() {
		return updatedValue;
	}

	long getTimestamp() {
		return timestamp;
	}

	@Override
	public String toString() {
		return "TtlUpdateContext{" +
			"key=" + key +
			", verifierId='" + verifierId + '\'' +
			", valueBeforeUpdate=" + valueBeforeUpdate +
			", update=" + update +
			", updatedValue=" + updatedValue +
			", timestamp=" + timestamp +
			'}';
	}
}
