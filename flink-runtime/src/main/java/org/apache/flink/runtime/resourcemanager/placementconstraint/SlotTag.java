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

package org.apache.flink.runtime.resourcemanager.placementconstraint;

import org.apache.flink.api.common.JobID;

import static org.apache.flink.runtime.resourcemanager.placementconstraint.SlotTagScope.JOB;

/**
 * A tag on a slot request / allocated slot.
 *
 * <p>SlotTags can be matched at different {@link SlotTagScope}s.
 */
public class SlotTag {
	private final String tagName;
	private final JobID jobId;

	public SlotTag(String tagName, JobID jobId) {
		this.tagName = tagName;
		this.jobId = jobId;
	}

	public String getTagName() { return tagName; }
	public JobID getJobId() { return  jobId; }

	public boolean match(SlotTag tag, SlotTagScope scope) {
		switch (scope) {
			case JOB:
				return jobId.equals(tag.getJobId()) && tagName.equals(tag.getTagName());
			case FLINK:
				return tagName.equals(tag.getTagName());
			default:
				// should never happen
				throw new RuntimeException("Unknown slot tag scope {}.");
		}
	}

	public boolean match(SlotTag tag) {
		return match(tag, JOB);
	}

	@Override
	public boolean equals(Object obj) {
		SlotTag tag = (SlotTag) obj;
		return tagName.equals(tag.tagName) && jobId.equals(tag.jobId);
	}
}
