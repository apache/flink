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

package org.apache.flink.streaming.api.operators.collect;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.operators.collect.utils.CollectSinkFunctionTestWrapper;
import org.apache.flink.streaming.api.operators.collect.utils.CollectTestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CollectSinkFunction}. */
class CollectSinkFunctionTest {

    private static final TypeSerializer<Integer> serializer = IntSerializer.INSTANCE;

    private CollectSinkFunctionTestWrapper<Integer> functionWrapper;

    @BeforeEach
    void before() throws Exception {
        // max bytes per batch = 3 * sizeof(int)
        functionWrapper = new CollectSinkFunctionTestWrapper<>(serializer, 12);
    }

    @AfterEach
    void after() throws Exception {
        functionWrapper.closeWrapper();
    }

    @Test
    void testIncreasingToken() throws Exception {
        functionWrapper.openFunction();
        for (int i = 0; i < 6; i++) {
            functionWrapper.invoke(i);
        }
        String version = initializeVersion();

        CollectCoordinationResponse response;
        response = functionWrapper.sendRequestAndGetResponse(version, 0);
        assertResponseEquals(response, version, 0, Arrays.asList(0, 1, 2));
        response = functionWrapper.sendRequestAndGetResponse(version, 4);
        assertResponseEquals(response, version, 0, Arrays.asList(4, 5));
        response = functionWrapper.sendRequestAndGetResponse(version, 6);
        assertResponseEquals(response, version, 0, Collections.emptyList());

        functionWrapper.closeFunctionNormally();
    }

    @Test
    void testDuplicatedToken() throws Exception {
        functionWrapper.openFunction();
        for (int i = 0; i < 6; i++) {
            functionWrapper.invoke(i);
        }
        String version = initializeVersion();

        CollectCoordinationResponse response;
        response = functionWrapper.sendRequestAndGetResponse(version, 0);
        assertResponseEquals(response, version, 0, Arrays.asList(0, 1, 2));
        response = functionWrapper.sendRequestAndGetResponse(version, 4);
        assertResponseEquals(response, version, 0, Arrays.asList(4, 5));
        response = functionWrapper.sendRequestAndGetResponse(version, 4);
        assertResponseEquals(response, version, 0, Arrays.asList(4, 5));

        functionWrapper.closeFunctionNormally();
    }

    @Test
    void testInvalidToken() throws Exception {
        functionWrapper.openFunction();
        for (int i = 0; i < 6; i++) {
            functionWrapper.invoke(i);
        }
        String version = initializeVersion();
        functionWrapper.sendRequestAndGetResponse(version, 4);

        // invalid token
        CollectCoordinationResponse response =
                functionWrapper.sendRequestAndGetResponse(version, 3);
        assertResponseEquals(response, version, 0, Collections.emptyList());

        functionWrapper.closeFunctionNormally();
    }

    @Test
    void testInvalidVersion() throws Exception {
        functionWrapper.openFunction();
        for (int i = 0; i < 6; i++) {
            functionWrapper.invoke(i);
        }
        String version = initializeVersion();

        // invalid version
        CollectCoordinationResponse response =
                functionWrapper.sendRequestAndGetResponse("invalid version", 0);
        assertResponseEquals(response, version, 0, Collections.emptyList());

        functionWrapper.closeFunctionNormally();
    }

    @Test
    void testConfiguredPortIsUsed() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            functionWrapper
                    .getRuntimeContext()
                    .getTaskManagerRuntimeInfo()
                    .getConfiguration()
                    .set(TaskManagerOptions.COLLECT_PORT, socket.getLocalPort());
            assertThatThrownBy(() -> functionWrapper.openFunction())
                    .isInstanceOf(BindException.class)
                    .hasMessageContaining("Address already in use");
        }
    }

    @Test
    void testCheckpoint() throws Exception {
        functionWrapper.openFunctionWithState();
        for (int i = 0; i < 2; i++) {
            functionWrapper.invoke(i);
        }
        String version = initializeVersion();

        CollectCoordinationResponse response =
                functionWrapper.sendRequestAndGetResponse(version, 0);
        assertResponseEquals(response, version, 0, Arrays.asList(0, 1));

        for (int i = 2; i < 6; i++) {
            functionWrapper.invoke(i);
        }

        response = functionWrapper.sendRequestAndGetResponse(version, 3);
        assertResponseEquals(response, version, 0, Arrays.asList(3, 4, 5));

        functionWrapper.checkpointFunction(1);

        // checkpoint hasn't finished yet
        response = functionWrapper.sendRequestAndGetResponse(version, 4);
        assertResponseEquals(response, version, 0, Arrays.asList(4, 5));

        functionWrapper.checkpointComplete(1);

        // checkpoint finished
        response = functionWrapper.sendRequestAndGetResponse(version, 4);
        assertResponseEquals(response, version, 3, Arrays.asList(4, 5));

        functionWrapper.closeFunctionNormally();
    }

    @Test
    void testRestart() throws Exception {
        functionWrapper.openFunctionWithState();
        for (int i = 0; i < 3; i++) {
            functionWrapper.invoke(i);
        }
        String version = initializeVersion();

        functionWrapper.sendRequestAndGetResponse(version, 1);
        functionWrapper.checkpointFunction(1);
        functionWrapper.checkpointComplete(1);

        CollectCoordinationResponse response =
                functionWrapper.sendRequestAndGetResponse(version, 1);
        assertResponseEquals(response, version, 1, Arrays.asList(1, 2));

        // these records are not checkpointed
        for (int i = 3; i < 6; i++) {
            functionWrapper.invoke(i);
        }
        response = functionWrapper.sendRequestAndGetResponse(version, 2);
        assertResponseEquals(response, version, 1, Arrays.asList(2, 3, 4));

        functionWrapper.closeFunctionAbnormally();
        functionWrapper.openFunctionWithState();
        version = initializeVersion();

        response = functionWrapper.sendRequestAndGetResponse(version, 1);
        assertResponseEquals(response, version, 1, Arrays.asList(1, 2));

        for (int i = 6; i < 9; i++) {
            functionWrapper.invoke(i);
        }
        response = functionWrapper.sendRequestAndGetResponse(version, 2);
        assertResponseEquals(response, version, 1, Arrays.asList(2, 6, 7));

        functionWrapper.closeFunctionNormally();
    }

    @Test
    void testAccumulatorResultWithoutCheckpoint() throws Exception {
        testAccumulatorResultWithoutCheckpoint(2, Arrays.asList(2, 3, 4, 5));
    }

    @Test
    void testEmptyAccumulatorResult() throws Exception {
        testAccumulatorResultWithoutCheckpoint(6, Collections.emptyList());
    }

    private void testAccumulatorResultWithoutCheckpoint(int offset, List<Integer> expected)
            throws Exception {
        functionWrapper.openFunction();
        for (int i = 0; i < 6; i++) {
            functionWrapper.invoke(i);
        }
        String version = initializeVersion();
        functionWrapper.sendRequestAndGetResponse(version, offset);

        functionWrapper.closeFunctionNormally();
        CollectTestUtils.assertAccumulatorResult(
                functionWrapper.getAccumulatorResults(), offset, version, 0, expected, serializer);
    }

    @Test
    void testAccumulatorResultWithCheckpoint() throws Exception {
        functionWrapper.openFunctionWithState();
        for (int i = 0; i < 6; i++) {
            functionWrapper.invoke(i);
        }
        String version = initializeVersion();
        functionWrapper.sendRequestAndGetResponse(version, 3);

        functionWrapper.checkpointFunction(1);
        functionWrapper.checkpointComplete(1);

        for (int i = 6; i < 9; i++) {
            functionWrapper.invoke(i);
        }
        functionWrapper.sendRequestAndGetResponse(version, 5);

        functionWrapper.closeFunctionNormally();
        CollectTestUtils.assertAccumulatorResult(
                functionWrapper.getAccumulatorResults(),
                5,
                version,
                3,
                Arrays.asList(5, 6, 7, 8),
                serializer);
    }

    private String initializeVersion() throws Exception {
        CollectCoordinationResponse response = functionWrapper.sendRequestAndGetResponse("", 0);
        return response.getVersion();
    }

    private void assertResponseEquals(
            CollectCoordinationResponse response,
            String version,
            long lastCheckpointedOffset,
            List<Integer> expected)
            throws IOException {
        CollectTestUtils.assertResponseEquals(
                response, version, lastCheckpointedOffset, expected, serializer);
    }
}
