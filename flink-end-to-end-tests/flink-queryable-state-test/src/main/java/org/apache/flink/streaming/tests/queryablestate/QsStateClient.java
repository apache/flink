/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.tests.queryablestate;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.queryablestate.exceptions.UnknownKeyOrNamespaceException;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * A simple implementation of a queryable state client. This client queries the state for a while
 * (~2.5 mins) and prints out the values that it found in the map state
 *
 * <p>Usage: java -jar QsStateClient.jar --host HOST --port PORT --job-id JOB_ID
 */
public class QsStateClient {

    private static final int BOOTSTRAP_RETRIES = 240;

    public static void main(final String[] args) throws Exception {

        ParameterTool parameters = ParameterTool.fromArgs(args);

        // setup values
        String jobId = parameters.getRequired("job-id");
        String host = parameters.get("host", "localhost");
        int port = parameters.getInt("port", 9069);
        int numIterations = parameters.getInt("iterations", 1500);

        QueryableStateClient client = new QueryableStateClient(host, port);
        client.setExecutionConfig(new ExecutionConfig());

        MapStateDescriptor<EmailId, EmailInformation> stateDescriptor =
                new MapStateDescriptor<>(
                        QsConstants.STATE_NAME,
                        TypeInformation.of(new TypeHint<EmailId>() {}),
                        TypeInformation.of(new TypeHint<EmailInformation>() {}));

        System.out.println("Wait until the state can be queried.");

        // wait for state to exist
        for (int i = 0; i < BOOTSTRAP_RETRIES; i++) { // ~120s
            try {
                getMapState(jobId, client, stateDescriptor);
                break;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnknownKeyOrNamespaceException) {
                    System.err.println("State does not exist yet; sleeping 500ms");
                    Thread.sleep(500L);
                } else {
                    throw e;
                }
            }

            if (i == (BOOTSTRAP_RETRIES - 1)) {
                throw new RuntimeException("Timeout: state doesn't exist after 120s");
            }
        }

        System.out.println(
                String.format("State exists. Start querying it %d times.", numIterations));

        // query state
        for (int iterations = 0; iterations < numIterations; iterations++) {

            MapState<EmailId, EmailInformation> mapState =
                    getMapState(jobId, client, stateDescriptor);

            int counter = 0;
            for (Map.Entry<EmailId, EmailInformation> entry : mapState.entries()) {
                // this is to force deserialization
                entry.getKey();
                entry.getValue();
                counter++;
            }
            System.out.println(
                    "MapState has " + counter + " entries"); // we look for it in the test

            Thread.sleep(100L);
        }
    }

    private static MapState<EmailId, EmailInformation> getMapState(
            String jobId,
            QueryableStateClient client,
            MapStateDescriptor<EmailId, EmailInformation> stateDescriptor)
            throws InterruptedException, ExecutionException {

        CompletableFuture<MapState<EmailId, EmailInformation>> resultFuture =
                client.getKvState(
                        JobID.fromHexString(jobId),
                        QsConstants.QUERY_NAME,
                        QsConstants.KEY, // which key of the keyed state to access
                        BasicTypeInfo.STRING_TYPE_INFO,
                        stateDescriptor);

        return resultFuture.get();
    }
}
