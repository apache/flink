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

package org.apache.flink.runtime.webmonitor;

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.messages.webmonitor.JobsWithIDsOverview;
import org.apache.flink.runtime.messages.webmonitor.StatusOverview;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * This class implements the utility methods that convert the responses into JSON strings.
 */
public class JsonFactory {

	private static final com.fasterxml.jackson.core.JsonFactory jacksonFactory =
			new com.fasterxml.jackson.core.JsonFactory();

	public static String generateConfigJSON(long refreshInterval, long timeZoneOffset, String timeZoneName) {
		try {
			StringWriter writer = new StringWriter();
			JsonGenerator gen = jacksonFactory.createJsonGenerator(writer);
			
			gen.writeStartObject();
			gen.writeNumberField("refresh-interval", refreshInterval);
			gen.writeNumberField("timezone-offset", timeZoneOffset);
			gen.writeStringField("timezone-name", timeZoneName);
			gen.writeEndObject();
			
			gen.close();
			return writer.toString();
		}
		catch (Exception e) {
			// this should not happen
			throw new RuntimeException(e.getMessage(), e);
		}
	}
	
	public static String generateOverviewJSON(StatusOverview overview) {
		try {
			StringWriter writer = new StringWriter();
			JsonGenerator gen = jacksonFactory.createJsonGenerator(writer);

			gen.writeStartObject();
			gen.writeNumberField("taskmanagers", overview.getNumTaskManagersConnected());
			gen.writeNumberField("slots-total", overview.getNumSlotsTotal());
			gen.writeNumberField("slots-available", overview.getNumSlotsAvailable());
			gen.writeNumberField("jobs-running", overview.getNumJobsRunningOrPending());
			gen.writeNumberField("jobs-finished", overview.getNumJobsFinished());
			gen.writeNumberField("jobs-cancelled", overview.getNumJobsCancelled());
			gen.writeNumberField("jobs-failed", overview.getNumJobsFailed());
			gen.writeEndObject();

			gen.close();
			return writer.toString();
		}
		catch (Exception e) {
			// this should not happen
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	
	public static String generateJobsOverviewJSON(JobsWithIDsOverview overview) {
		try {
			List<JobID> runningIDs = overview.getJobsRunningOrPending();
			List<String> runningStrings = new ArrayList<String>(runningIDs.size());
			for (JobID jid : runningIDs) {
				runningStrings.add(jid.toString());
			}

			List<JobID> finishedIDs = overview.getJobsFinished();
			List<String> finishedStrings = new ArrayList<String>(finishedIDs.size());
			for (JobID jid : finishedIDs) {
				finishedStrings.add(jid.toString());
			}

			List<JobID> canceledIDs = overview.getJobsCancelled();
			List<String> canceledStrings = new ArrayList<String>(canceledIDs.size());
			for (JobID jid : canceledIDs) {
				canceledStrings.add(jid.toString());
			}

			List<JobID> failedIDs = overview.getJobsFailed();
			List<String> failedStrings = new ArrayList<String>(failedIDs.size());
			for (JobID jid : failedIDs) {
				failedStrings.add(jid.toString());
			}

			JSONObject response = new JSONObject();
			response.put("jobs-running", runningStrings);
			response.put("jobs-finished", finishedStrings);
			response.put("jobs-cancelled", canceledStrings);
			response.put("jobs-failed", failedStrings);
			return response.toString(2);
		}
		catch (JSONException e) {
			// this should not happen
			throw new RuntimeException(e);
		}
	}
	
	public static String createJobSummaryJSON(JobID jid, String jobName, String state,
												String start, String end, String duration,
												int numOperators, int numOperatorsPending,
												int numOperatorsRunning, int numOperatorsFinished,
												int numOperatorsCanceling, int numOperatorsCanceled,
												int numOperatorsFailed) {
		try {
			JSONObject json = new JSONObject();

			json.put("jid", jid.toString());
			json.put("name", jobName);
			json.put("state", state);
			json.put("start-time", start);
			json.put("end-time", end);
			json.put("duration", duration);
			
			JSONObject operators = new JSONObject();
			operators.put("total", numOperators);
			operators.put("pending", numOperatorsPending);
			operators.put("running", numOperatorsRunning);
			operators.put("finished", numOperatorsFinished);
			operators.put("canceling", numOperatorsCanceling);
			operators.put("canceled", numOperatorsCanceled);
			operators.put("failed", numOperatorsFailed);
			json.put("operators", operators);
			
			return json.toString(2);
		}
		catch (JSONException e) {
			// this should not happen
			throw new RuntimeException(e);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	/** Don't instantiate */
	private JsonFactory() {}
}
