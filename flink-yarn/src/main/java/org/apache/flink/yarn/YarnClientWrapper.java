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

import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.Objects;

/** Wrapper class for {@link YarnClient}. */
@Internal
public class YarnClientWrapper implements AutoCloseable {
    private YarnClient yarnClient;
    private boolean allowToStop;

    public YarnClientWrapper(YarnClient yarnClient, boolean allowToStop) {
        this.yarnClient = yarnClient;
        this.allowToStop = allowToStop;
    }

    public ApplicationReport getApplicationReport(ApplicationId appId)
            throws YarnException, IOException {
        return yarnClient.getApplicationReport(appId);
    }

    @Override
    public void close() throws Exception {
        if (allowToStop) {
            yarnClient.close();
        }
    }

    public boolean isClosed() {
        return yarnClient.isInState(Service.STATE.STOPPED);
    }

    public static YarnClientWrapper of(YarnClient yarnClient, boolean allowToStop) {
        return new YarnClientWrapper(yarnClient, allowToStop);
    }

    public static YarnClientWrapper of(YarnConfiguration yarnConfiguration, boolean allowToStop) {
        final YarnClient newlyCreatedYarnClient = YarnClient.createYarnClient();
        newlyCreatedYarnClient.init(yarnConfiguration);
        newlyCreatedYarnClient.start();
        return of(newlyCreatedYarnClient, allowToStop);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        YarnClientWrapper that = (YarnClientWrapper) o;
        return allowToStop == that.allowToStop && yarnClient.equals(that.yarnClient);
    }

    @Override
    public int hashCode() {
        return Objects.hash(yarnClient, allowToStop);
    }
}
