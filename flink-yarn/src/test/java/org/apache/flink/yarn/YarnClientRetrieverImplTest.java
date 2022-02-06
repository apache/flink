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

package org.apache.flink.yarn;

import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link YarnClientRetrieverImpl}. */
@ExtendWith(TestLoggerExtension.class)
public class YarnClientRetrieverImplTest {

    @Test
    public void testDedicatedYarnClientClosedWillCreateNewOne() throws Exception {
        YarnConfiguration yarnConfiguration = new YarnConfiguration();
        YarnClientRetrieverImpl retriever = YarnClientRetrieverImpl.from(yarnConfiguration);
        YarnClientWrapper yarnClient1 = retriever.getYarnClient();
        yarnClient1.close();

        assertTrue(yarnClient1.isClosed());

        YarnClientWrapper yarnClient2 = retriever.getYarnClient();
        assertNotEquals(yarnClient1, yarnClient2);
    }

    @Test
    public void testExternalYarnClientAllowNotStop() throws Exception {
        YarnConfiguration yarnConfiguration = new YarnConfiguration();
        YarnClient externalYarnClient = createYarnClient(yarnConfiguration);

        YarnClientRetrieverImpl retriever =
                YarnClientRetrieverImpl.from(
                        YarnClientWrapper.fromBorrowed(externalYarnClient), yarnConfiguration);
        retriever.getYarnClient().close();

        assertFalse(externalYarnClient.isInState(Service.STATE.STOPPED));
    }

    @Test
    public void testExternalYarnClientWillBeReusedWhenNotClose() throws FlinkException {
        final YarnConfiguration yarnConfiguration = new YarnConfiguration();
        final YarnClient yarnClient = createYarnClient(yarnConfiguration);

        final YarnClientWrapper wrapper = YarnClientWrapper.fromBorrowed(yarnClient);

        YarnClientRetrieverImpl retriever =
                YarnClientRetrieverImpl.from(wrapper, yarnConfiguration);

        assertEquals(wrapper, retriever.getYarnClient());
    }

    @Test
    public void testExternalYarnClientClosedWillCreateDedicatedYarnClient() throws Exception {
        YarnConfiguration yarnConfiguration = new YarnConfiguration();
        YarnClient yarnClient = createYarnClient(yarnConfiguration);
        yarnClient.stop();

        final YarnClientWrapper wrapper = YarnClientWrapper.fromBorrowed(yarnClient);

        YarnClientRetrieverImpl retriever =
                YarnClientRetrieverImpl.from(wrapper, yarnConfiguration);

        assertNotEquals(wrapper, retriever.getYarnClient());
    }

    private YarnClient createYarnClient(YarnConfiguration yarnConfiguration) {
        final YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConfiguration);
        yarnClient.start();
        return yarnClient;
    }
}
