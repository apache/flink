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

package org.apache.flink.runtime.rest.messages.checkpoints;

import org.apache.flink.runtime.rest.handler.job.checkpoints.CheckpointConfigHandler;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.Objects;

/**
 * Response class of the {@link CheckpointConfigHandler}.
 */
public class CheckpointConfigInfo implements ResponseBody {

	public static final String FIELD_NAME_PROCESSING_MODE = "mode";

	public static final String FIELD_NAME_CHECKPOINT_INTERVAL = "interval";

	public static final String FIELD_NAME_CHECKPOINT_TIMEOUT = "timeout";

	public static final String FIELD_NAME_CHECKPOINT_MIN_PAUSE = "min_pause";

	public static final String FIELD_NAME_CHECKPOINT_MAX_CONCURRENT = "max_concurrent";

	public static final String FIELD_NAME_EXTERNALIZED_CHECKPOINT_CONFIG = "externalization";

	public static final String FIELD_NAME_STATE_BACKEND = "state_backend";

	@JsonProperty(FIELD_NAME_PROCESSING_MODE)
	private final ProcessingMode processingMode;

	@JsonProperty(FIELD_NAME_CHECKPOINT_INTERVAL)
	private final long checkpointInterval;

	@JsonProperty(FIELD_NAME_CHECKPOINT_TIMEOUT)
	private final long checkpointTimeout;

	@JsonProperty(FIELD_NAME_CHECKPOINT_MIN_PAUSE)
	private final long minPauseBetweenCheckpoints;

	@JsonProperty(FIELD_NAME_CHECKPOINT_MAX_CONCURRENT)
	private final long maxConcurrentCheckpoints;

	@JsonProperty(FIELD_NAME_EXTERNALIZED_CHECKPOINT_CONFIG)
	private final ExternalizedCheckpointInfo externalizedCheckpointInfo;

	@JsonProperty(FIELD_NAME_STATE_BACKEND)
	private final String stateBackend;

	@JsonCreator
	public CheckpointConfigInfo(
			@JsonProperty(FIELD_NAME_PROCESSING_MODE) ProcessingMode processingMode,
			@JsonProperty(FIELD_NAME_CHECKPOINT_INTERVAL) long checkpointInterval,
			@JsonProperty(FIELD_NAME_CHECKPOINT_TIMEOUT) long checkpointTimeout,
			@JsonProperty(FIELD_NAME_CHECKPOINT_MIN_PAUSE) long minPauseBetweenCheckpoints,
			@JsonProperty(FIELD_NAME_CHECKPOINT_MAX_CONCURRENT) int maxConcurrentCheckpoints,
			@JsonProperty(FIELD_NAME_EXTERNALIZED_CHECKPOINT_CONFIG) ExternalizedCheckpointInfo externalizedCheckpointInfo,
			@JsonProperty(FIELD_NAME_STATE_BACKEND) String stateBackend) {
		this.processingMode = Preconditions.checkNotNull(processingMode);
		this.checkpointInterval = checkpointInterval;
		this.checkpointTimeout = checkpointTimeout;
		this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
		this.maxConcurrentCheckpoints = maxConcurrentCheckpoints;
		this.externalizedCheckpointInfo = Preconditions.checkNotNull(externalizedCheckpointInfo);
		this.stateBackend = Preconditions.checkNotNull(stateBackend);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		CheckpointConfigInfo that = (CheckpointConfigInfo) o;
		return checkpointInterval == that.checkpointInterval &&
			checkpointTimeout == that.checkpointTimeout &&
			minPauseBetweenCheckpoints == that.minPauseBetweenCheckpoints &&
			maxConcurrentCheckpoints == that.maxConcurrentCheckpoints &&
			processingMode == that.processingMode &&
			Objects.equals(externalizedCheckpointInfo, that.externalizedCheckpointInfo) &&
			Objects.equals(stateBackend, that.stateBackend);
	}

	@Override
	public int hashCode() {
		return Objects.hash(processingMode, checkpointInterval, checkpointTimeout, minPauseBetweenCheckpoints,
			maxConcurrentCheckpoints, externalizedCheckpointInfo, stateBackend);
	}

	/**
	 * Contains information about the externalized checkpoint configuration.
	 */
	public static final class ExternalizedCheckpointInfo {

		public static final String FIELD_NAME_ENABLED = "enabled";

		public static final String FIELD_NAME_DELETE_ON_CANCELLATION = "delete_on_cancellation";

		@JsonProperty(FIELD_NAME_ENABLED)
		private final boolean enabled;

		@JsonProperty(FIELD_NAME_DELETE_ON_CANCELLATION)
		private final boolean deleteOnCancellation;

		@JsonCreator
		public ExternalizedCheckpointInfo(
				@JsonProperty(FIELD_NAME_ENABLED) boolean enabled,
				@JsonProperty(FIELD_NAME_DELETE_ON_CANCELLATION) boolean deleteOnCancellation) {
			this.enabled = enabled;
			this.deleteOnCancellation = deleteOnCancellation;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			ExternalizedCheckpointInfo that = (ExternalizedCheckpointInfo) o;
			return enabled == that.enabled &&
				deleteOnCancellation == that.deleteOnCancellation;
		}

		@Override
		public int hashCode() {
			return Objects.hash(enabled, deleteOnCancellation);
		}
	}

	/**
	 * Processing mode.
	 */
	@JsonSerialize(using = ProcessingModeSerializer.class)
	@JsonDeserialize(using = ProcessingModeDeserializer.class)
	public enum ProcessingMode {
		AT_LEAST_ONCE,
		EXACTLY_ONCE
	}

	/**
	 * JSON deserializer for {@link ProcessingMode}.
	 */
	public static class ProcessingModeSerializer extends StdSerializer<ProcessingMode> {

		public ProcessingModeSerializer() {
			super(ProcessingMode.class);
		}

		@Override
		public void serialize(ProcessingMode mode, JsonGenerator generator, SerializerProvider serializerProvider)
			throws IOException {
			generator.writeString(mode.name().toLowerCase());
		}
	}

	/**
	 * Processing mode deserializer.
	 */
	public static class ProcessingModeDeserializer extends StdDeserializer<ProcessingMode> {

		public ProcessingModeDeserializer() {
			super(ProcessingMode.class);
		}

		@Override
		public ProcessingMode deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
			throws IOException {
			return ProcessingMode.valueOf(jsonParser.getValueAsString().toUpperCase());
		}
	}

}
