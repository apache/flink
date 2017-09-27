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

package org.apache.flink.runtime.messages.webmonitor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.rest.messages.ResponseBody;

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
import java.util.ArrayList;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An overview of how many jobs are in which status.
 */
public class JobIdsWithStatusesOverview implements ResponseBody, InfoMessage {

	private static final long serialVersionUID = -3699051943490133183L;

	public static final String FIELD_NAME_JOB_IDS = "job-ids";

	@JsonProperty(FIELD_NAME_JOB_IDS)
	@JsonSerialize(contentUsing = JobIdsWithStatusesOverview.JobIdWithStatusSerializer.class)
	private final Collection<Tuple2<JobID, JobStatus>> jobIds;

	@JsonCreator
	public JobIdsWithStatusesOverview(
			@JsonProperty(FIELD_NAME_JOB_IDS) @JsonDeserialize(contentUsing = JobIdsWithStatusesOverview.JobIdWithStatusDeserializer.class) Collection<Tuple2<JobID, JobStatus>> jobIds) {
		this.jobIds = checkNotNull(jobIds);
	}

	public JobIdsWithStatusesOverview(JobIdsWithStatusesOverview first, JobIdsWithStatusesOverview second) {
		this.jobIds = combine(first.getJobIds(), second.getJobIds());
	}

	public Collection<Tuple2<JobID, JobStatus>> getJobIds() {
		return jobIds;
	}

	// ------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return jobIds.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		else if (obj instanceof JobIdsWithStatusesOverview) {
			JobIdsWithStatusesOverview that = (JobIdsWithStatusesOverview) obj;
			return jobIds.equals(that.getJobIds());
		}
		else {
			return false;
		}
	}

	@Override
	public String toString() {
		return "JobIdsWithStatusesOverview { " + jobIds + " }";
	}

	public static final class JobIdWithStatusSerializer extends StdSerializer<Tuple2<JobID, JobStatus>> {

		private static final long serialVersionUID = 2196011372021674535L;

		public JobIdWithStatusSerializer() {
			super(Tuple2.class, true);
		}

		@Override
		public void serialize(
				Tuple2<JobID, JobStatus> jobIdWithStatus,
				JsonGenerator jsonGenerator,
				SerializerProvider serializerProvider) throws IOException {

			jsonGenerator.writeStartArray();

			jsonGenerator.writeString(jobIdWithStatus.f0.toString());
			jsonGenerator.writeString(jobIdWithStatus.f1.name());

			jsonGenerator.writeEndArray();
		}
	}

	public static final class JobIdWithStatusDeserializer extends StdDeserializer<Tuple2<JobID, JobStatus>> {

		private static final long serialVersionUID = 5378670283978134794L;

		public JobIdWithStatusDeserializer() {
			super(Tuple2.class);
		}

		@Override
		public Tuple2<JobID, JobStatus> deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
			Tuple2<JobID, JobStatus> deserializedIdWithStatus = Tuple2.of(
				JobID.fromHexString(jsonParser.nextTextValue()), JobStatus.valueOf(jsonParser.nextTextValue()));

			// read the END_ARRAY token, otherwise it will be mistaken as the end of the whole collection of ids and statuses
			jsonParser.nextValue();

			return deserializedIdWithStatus;
		}
	}

	// ------------------------------------------------------------------------

	private static Collection<Tuple2<JobID, JobStatus>> combine(
			Collection<Tuple2<JobID, JobStatus>> first,
			Collection<Tuple2<JobID, JobStatus>> second) {

		checkNotNull(first);
		checkNotNull(second);
		ArrayList<Tuple2<JobID, JobStatus>> result = new ArrayList<>(first.size() + second.size());
		result.addAll(first);
		result.addAll(second);
		return result;
	}
}
