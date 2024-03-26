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

package org.apache.flink.runtime.highavailability.zookeeper;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.highavailability.ReusableClientHAServices;
import org.apache.flink.runtime.highavailability.ReusableClientHAServicesFactory;
import org.apache.flink.runtime.webmonitor.retriever.LeaderRetriever;
import org.apache.flink.util.FlinkException;

/** Factory for creating reusable client high availability services. */
public class DefaultReusableClientHAServicesFactory implements ReusableClientHAServicesFactory {

    public static final DefaultReusableClientHAServicesFactory INSTANCE =
            new DefaultReusableClientHAServicesFactory();

    @Override
    public ReusableClientHAServices createReusableClientHAServices(Configuration configuration)
            throws Exception {
        LeaderRetriever leaderRetriever = new LeaderRetriever();

        return HighAvailabilityServicesUtils.createReusableClientHAService(
                configuration,
                leaderRetriever,
                exception ->
                        leaderRetriever.handleError(
                                new FlinkException(
                                        "Fatal error happened with client HA " + "services.",
                                        exception)));
    }
}
