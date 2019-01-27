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

package org.apache.flink.runtime.update;

import org.apache.flink.runtime.update.action.JobUpdateAction;

import java.io.Serializable;
import java.util.List;

/**
 * Request containing a list of ordered job update actions. The latter action has higher priorities and can
 * override prior ones if they have conflicts.
 */
public class JobUpdateRequest implements Serializable {

	private List<JobUpdateAction> jobUpdateActions;

	public JobUpdateRequest(List<JobUpdateAction> actions) {
		this.jobUpdateActions = actions;
	}

	public List<JobUpdateAction> getJobUpdateActions() {
		return jobUpdateActions;
	}
}
