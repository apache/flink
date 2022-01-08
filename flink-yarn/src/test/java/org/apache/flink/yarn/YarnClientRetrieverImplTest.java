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

import org.apache.flink.util.TestLoggerExtension;

import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link YarnClientRetrieverImpl}. */
@ExtendWith(TestLoggerExtension.class)
public class YarnClientRetrieverImplTest {

    @Test
    public void testYarnClientWillBeReusedWhenNotClose() {
        YarnConfiguration yarnConfiguration = new YarnConfiguration();
        YarnClient yarnClient = createYarnClient(yarnConfiguration);

        YarnClientRetrieverImpl retriever =
                YarnClientRetrieverImpl.from(yarnClient, yarnConfiguration);
        assertEquals(yarnClient, retriever.getYarnClient());
    }

    @Test
    public void testYarnClientCloseWillNewCreate() throws Exception {
        YarnConfiguration yarnConfiguration = new YarnConfiguration();
        YarnClient yarnClient = createYarnClient(yarnConfiguration);
        yarnClient.stop();

        YarnClientRetrieverImpl retriever =
                YarnClientRetrieverImpl.from(yarnClient, yarnConfiguration);
        YarnClient newYarnClient = retriever.getYarnClient();

        assertNotEquals(yarnClient, newYarnClient);
        assertEquals(newYarnClient, retriever.getYarnClient());

        retriever.close();
        assertTrue(newYarnClient.isInState(Service.STATE.STOPPED));
    }

    private YarnClient createYarnClient(YarnConfiguration yarnConfiguration) {
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConfiguration);
        yarnClient.start();
        return yarnClient;
    }
}
