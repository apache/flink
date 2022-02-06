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
import org.apache.flink.util.Reference;

import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

/** Wrapper class for {@link YarnClient}. */
@Internal
public class YarnClientWrapper implements AutoCloseable {
    private Reference<YarnClient> yarnClientRef;

    public YarnClientWrapper(Reference<YarnClient> yarnClientRef) {
        this.yarnClientRef = yarnClientRef;
    }

    public ApplicationReport getApplicationReport(ApplicationId appId)
            throws YarnException, IOException {
        return yarnClientRef.deref().getApplicationReport(appId);
    }

    @Override
    public void close() throws Exception {
        if (yarnClientRef.isOwned()) {
            yarnClientRef.deref().close();
        }
    }

    public boolean isClosed() {
        return yarnClientRef.deref().isInState(Service.STATE.STOPPED);
    }

    public static YarnClientWrapper fromBorrowed(YarnClient yarnClient) {
        return new YarnClientWrapper(Reference.borrowed(yarnClient));
    }

    public static YarnClientWrapper fromNewlyCreated(YarnConfiguration yarnConfiguration) {
        final YarnClient newlyCreatedYarnClient = YarnClient.createYarnClient();
        newlyCreatedYarnClient.init(yarnConfiguration);
        newlyCreatedYarnClient.start();
        return new YarnClientWrapper(Reference.owned(newlyCreatedYarnClient));
    }
}
