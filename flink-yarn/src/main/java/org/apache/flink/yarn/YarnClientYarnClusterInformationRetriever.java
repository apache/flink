/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.yarn;

import org.apache.flink.util.FlinkException;

import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

/**
 * {@link YarnClusterInformationRetriever} implementation which queries the Yarn cluster using a
 * {@link YarnClient} instance.
 */
public final class YarnClientYarnClusterInformationRetriever
        implements YarnClusterInformationRetriever {
    private final YarnClient yarnClient;

    private YarnClientYarnClusterInformationRetriever(YarnClient yarnClient) {
        this.yarnClient = yarnClient;
    }

    @Override
    public int getMaxVcores() throws FlinkException {
        try {
            return yarnClient.getNodeReports(NodeState.RUNNING).stream()
                    .mapToInt(report -> report.getCapability().getVirtualCores())
                    .max()
                    .orElse(0);
        } catch (YarnException | IOException e) {
            throw new FlinkException(
                    "Couldn't get cluster description, please check on the YarnConfiguration", e);
        }
    }

    public static YarnClientYarnClusterInformationRetriever create(YarnClient yarnClient) {
        return new YarnClientYarnClusterInformationRetriever(yarnClient);
    }
}
