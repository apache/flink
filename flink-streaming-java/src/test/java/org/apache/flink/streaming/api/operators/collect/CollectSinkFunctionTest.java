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
import org.apache.flink.streaming.api.operators.collect.utils.CollectSinkFunctionTestWrapper;
import org.apache.flink.streaming.api.operators.collect.utils.CollectTestUtils;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Tests for {@link CollectSinkFunction}. */
public class CollectSinkFunctionTest extends TestLogger {

    private static final TypeSerializer<Integer> serializer = IntSerializer.INSTANCE;

    private CollectSinkFunctionTestWrapper<Integer> functionWrapper;

    @Before
    public void before() throws Exception {
        // max bytes per batch = 3 * sizeof(int)
        functionWrapper = new CollectSinkFunctionTestWrapper<>(serializer, 12);
    }

    @After
    public void after() throws Exception {
        functionWrapper.closeWrapper();
    }

    @Test
    public void testIncreasingToken() throws Exception {
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
    public void testDuplicatedToken() throws Exception {
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
    public void testInvalidToken() throws Exception {
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
    public void testInvalidVersion() throws Exception {
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
    public void testCheckpoint() throws Exception {
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
    public void testRestart() throws Exception {
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
    public void testAccumulatorResultWithoutCheckpoint() throws Exception {
        testAccumulatorResultWithoutCheckpoint(2, Arrays.asList(2, 3, 4, 5));
    }

    @Test
    public void testEmptyAccumulatorResult() throws Exception {
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
    public void testAccumulatorResultWithCheckpoint() throws Exception {
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
