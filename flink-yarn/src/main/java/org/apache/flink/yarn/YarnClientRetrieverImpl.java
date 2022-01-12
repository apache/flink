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

import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import static org.apache.hadoop.yarn.client.api.YarnClient.createYarnClient;

/**
 * The implementation of {@link YarnClientRetriever} which is used to get yarn client for {@link
 * ApplicationReportProviderImpl}. When external yarn client is closed, it will create new yarn
 * client that its lifecycle will be managed in this retriever.
 */
@Internal
public final class YarnClientRetrieverImpl implements YarnClientRetriever {
    private static final Logger LOG = LoggerFactory.getLogger(YarnClientRetrieverImpl.class);

    @Nullable private final YarnClient externalYarnClient;
    private final YarnConfiguration yarnConfiguration;
    @Nullable private YarnClient createdYarnClient;
    private boolean closed = false;

    private YarnClientRetrieverImpl(
            @Nullable YarnClient yarnClient, YarnConfiguration yarnConfiguration) {
        this.externalYarnClient = yarnClient;
        this.yarnConfiguration = yarnConfiguration;
    }

    @Override
    public YarnClient getYarnClient() throws FlinkException {
        if (closed) {
            throw new FlinkException(
                    "This instance of YarnClientRetrieverImpl has released its resources, it can't be invoked again.");
        }

        if (externalYarnClient != null && !externalYarnClient.isInState(Service.STATE.STOPPED)) {
            return externalYarnClient;
        }

        if (createdYarnClient != null) {
            if (createdYarnClient.isInState(Service.STATE.STOPPED)) {
                throw new FlinkException(
                        "The newly created yarn client in YarnClientRetrieverImpl has been stopped. This should not happen.");
            }
            return createdYarnClient;
        }

        final YarnClient newlyCreatedYarnClient = createYarnClient();
        newlyCreatedYarnClient.init(yarnConfiguration);
        newlyCreatedYarnClient.start();
        this.createdYarnClient = newlyCreatedYarnClient;

        return createdYarnClient;
    }

    @Override
    public void close() throws Exception {
        if (createdYarnClient != null) {
            createdYarnClient.stop();
        }
        closed = true;
    }

    public static YarnClientRetrieverImpl from(
            YarnClient yarnClient, YarnConfiguration yarnConfiguration) {
        return new YarnClientRetrieverImpl(yarnClient, yarnConfiguration);
    }

    public static YarnClientRetrieverImpl from(YarnConfiguration yarnConfiguration) {
        return new YarnClientRetrieverImpl(null, yarnConfiguration);
    }
}
