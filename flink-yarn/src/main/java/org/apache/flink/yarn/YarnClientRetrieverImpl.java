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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.FlinkException;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * The implementation of {@link YarnClientRetriever} which is used to get a wrapper of yarn client
 * for {@link ApplicationReportProviderImpl}. When external yarn client is closed or nullable, it
 * will create a dedicated yarn client.
 */
@Internal
public final class YarnClientRetrieverImpl implements YarnClientRetriever {
    private static final Logger LOG = LoggerFactory.getLogger(YarnClientRetrieverImpl.class);

    @Nullable private final YarnClientWrapper externallyCreatedYarnClient;
    private final YarnConfiguration yarnConfiguration;
    @Nullable private YarnClientWrapper dedicatedYarnClient;

    private YarnClientRetrieverImpl(
            @Nullable YarnClientWrapper externallyCreatedYarnClient,
            YarnConfiguration yarnConfiguration) {
        this.externallyCreatedYarnClient = externallyCreatedYarnClient;
        this.yarnConfiguration = yarnConfiguration;
    }

    @Override
    public YarnClientWrapper getYarnClient() throws FlinkException {

        if (isRunning(externallyCreatedYarnClient)) {
            return externallyCreatedYarnClient;
        }

        if (isRunning(dedicatedYarnClient)) {
            return dedicatedYarnClient;
        }

        this.dedicatedYarnClient = YarnClientWrapper.fromNewlyCreated(yarnConfiguration);

        return dedicatedYarnClient;
    }

    private boolean isRunning(YarnClientWrapper yarnClient) {
        return yarnClient != null && !yarnClient.isClosed();
    }

    public static YarnClientRetrieverImpl from(
            YarnClientWrapper yarnClient, YarnConfiguration yarnConfiguration) {
        return new YarnClientRetrieverImpl(yarnClient, yarnConfiguration);
    }

    public static YarnClientRetrieverImpl from(YarnConfiguration yarnConfiguration) {
        return new YarnClientRetrieverImpl(null, yarnConfiguration);
    }
}
