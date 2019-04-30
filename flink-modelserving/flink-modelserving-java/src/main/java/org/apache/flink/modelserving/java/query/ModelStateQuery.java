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

package org.apache.flink.modelserving.java.query;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.modelserving.java.model.ModelToServeStats;
import org.apache.flink.queryablestate.client.QueryableStateClient;

import org.joda.time.DateTime;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * ModelStateQuery - query model state (works only for keyed implementation).
 */
public class ModelStateQuery {

    // Timeout between invocations
	private static final long defaulttimeInterval = 1000 * 20;        // 20 sec

	/**
	 * Query model state.
	 *
	 * @param job jobID.
	 * @param keys keys to query.
	 */
	public void query(String job, List<String> keys) throws Exception {
		query(job, keys, "127.0.0.1", 9069, defaulttimeInterval);
	}

	/**
	 * Query model state.
	 *
	 * @param job jobID.
	 * @param keys keys to query.
	 * @param host host to connect to.
	 * @param port port to use.
	 */
	public void query(String job, List<String> keys, String host, int port) throws Exception {
		query(job, keys, host, port, defaulttimeInterval);
	}

		/**
         * Query model state.
         *
         * @param job jobID.
         * @param keys keys to query.
         * @param host host to connect to.
         * @param port port to use.
         * @param timeInterval timeinterval for query.
         */
	public void query(String job, List<String> keys, String host, int port, long timeInterval) throws Exception {

        // Job ID, has to be active on the server
		JobID jobId = JobID.fromHexString(job);
        // Queryable client
		QueryableStateClient client = new QueryableStateClient(host, port);
        // the state descriptor of the state to be fetched.
		ValueStateDescriptor<ModelToServeStats> descriptor = new ValueStateDescriptor<>(
			"currentModel",   // state name
			TypeInformation.of(ModelToServeStats.class).createSerializer(new ExecutionConfig()) // type serializer
		);
        // Key type
		BasicTypeInfo keyType = BasicTypeInfo.STRING_TYPE_INFO;
		System.out.println("                   Name                      |       Description       |       Since       |       Average       |       Min       |       Max       |");
		while (true) {
			for (String key : keys) {
                // For every key obtain a corresponding state
				CompletableFuture<ValueState<ModelToServeStats>> future =
					client.getKvState(jobId, "currentModelState", key, keyType, descriptor)
						.handle((result, ex) -> {
							if (result != null) {
								return result;
							} else {
								System.err.println("Exception getting state : " + ex);
								return null;
							}
						});
				future.thenAccept(response -> {
					try {
						ModelToServeStats stats = response.value();
						System.out.println("  " + stats.getName() + " | " + stats.getDescription() + " | " +
							new DateTime(stats.getSince()).toString("yyyy/MM/dd HH:MM:SS") + " | " +
							stats.getDuration() / stats.getInvocations() + " |  " + stats.getMin() + " | " + stats.getMax() + " |");
					} catch (Exception e) {
						e.printStackTrace();
					}
				});
			}
            // Wait
			Thread.sleep(timeInterval);
		}
	}
}
