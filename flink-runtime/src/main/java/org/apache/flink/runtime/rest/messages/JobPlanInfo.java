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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.runtime.rest.handler.job.JobPlanHandler;
import org.apache.flink.util.Preconditions;

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
 * Response type of the {@link JobPlanHandler}.
 */
@JsonSerialize(using = JobPlanInfo.Serializer.class)
@JsonDeserialize(using = JobPlanInfo.Deserializer.class)
public class JobPlanInfo implements ResponseBody {

	private final String jsonPlan;

	public JobPlanInfo(String jsonPlan) {
		this.jsonPlan = Preconditions.checkNotNull(jsonPlan);
	}

	public String getJsonPlan() {
		return jsonPlan;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		JobPlanInfo that = (JobPlanInfo) o;
		return Objects.equals(jsonPlan, that.jsonPlan);
	}

	@Override
	public int hashCode() {
		return Objects.hash(jsonPlan);
	}

	//---------------------------------------------------------------------------------
	// Static helper classes
	//---------------------------------------------------------------------------------

	/**
	 * Json serializer for the {@link JobPlanInfo}.
	 */
	public static final class Serializer extends StdSerializer<JobPlanInfo> {

		private static final long serialVersionUID = -1551666039618928811L;

		public Serializer() {
			super(JobPlanInfo.class);
		}

		@Override
		public void serialize(
			JobPlanInfo jobPlanInfo,
			JsonGenerator jsonGenerator,
			SerializerProvider serializerProvider) throws IOException {
			jsonGenerator.writeString(jobPlanInfo.getJsonPlan());
		}
	}

	/**
	 * Json deserializer for the {@link JobPlanInfo}.
	 */
	public static final class Deserializer extends StdDeserializer<JobPlanInfo> {

		private static final long serialVersionUID = -3580088509877177213L;

		public Deserializer() {
			super(JobPlanInfo.class);
		}

		@Override
		public JobPlanInfo deserialize(
			JsonParser jsonParser,
			DeserializationContext deserializationContext) throws IOException {
			final String jsonPlan = jsonParser.getText();
			return new JobPlanInfo(jsonPlan);
		}
	}
}
