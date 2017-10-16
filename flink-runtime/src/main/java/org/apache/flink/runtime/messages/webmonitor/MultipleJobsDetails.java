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

import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.collections.CollectionUtils;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

/**
 * An actor messages describing details of various jobs. This message is sent for example
 * in response to the {@link RequestJobDetails} message.
 */
public class MultipleJobsDetails implements ResponseBody, Serializable {

	private static final long serialVersionUID = -1526236139616019127L;
	
	public static final String FIELD_NAME_JOBS_RUNNING = "running";
	public static final String FIELD_NAME_JOBS_FINISHED = "finished";

	@JsonProperty(FIELD_NAME_JOBS_RUNNING)
	private final Collection<JobDetails> running;

	@JsonProperty(FIELD_NAME_JOBS_FINISHED)
	private final Collection<JobDetails> finished;

	@JsonCreator
	public MultipleJobsDetails(
			@JsonProperty(FIELD_NAME_JOBS_RUNNING) Collection<JobDetails> running,
			@JsonProperty(FIELD_NAME_JOBS_FINISHED) Collection<JobDetails> finished) {
		this.running = running == null ? Collections.emptyList() : running;
		this.finished = finished == null ? Collections.emptyList() : finished;
	}
	
	// ------------------------------------------------------------------------

	public Collection<JobDetails> getRunning() {
		return running;
	}

	public Collection<JobDetails> getFinished() {
		return finished;
	}

	@Override
	public String toString() {
		return "MultipleJobsDetails{" +
			"running=" + running +
			", finished=" + finished +
			'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		MultipleJobsDetails that = (MultipleJobsDetails) o;

		return CollectionUtils.isEqualCollection(running, that.running) &&
			CollectionUtils.isEqualCollection(finished, that.finished);
	}

	@Override
	public int hashCode() {
		// the hash code only depends on the collection elements, not the collection itself!
		int result = 1;

		Iterator<JobDetails> iterator = running.iterator();

		while (iterator.hasNext()) {
			JobDetails jobDetails = iterator.next();
			result = 31 * result + (jobDetails == null ? 0 : jobDetails.hashCode());
		}

		iterator = finished.iterator();

		while (iterator.hasNext()) {
			JobDetails jobDetails = iterator.next();
			result = 31 * result + (jobDetails == null ? 0 : jobDetails.hashCode());
		}

		return result;
	}

	// ------------------------------------------------------------------------
}
