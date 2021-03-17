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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.util.FlinkRuntimeException;

/**
 * {@link JobGraphStoreFactory} implementation which creates a {@link JobGraphStore} using the
 * provided {@link HighAvailabilityServices}.
 */
public class HaServicesJobGraphStoreFactory implements JobGraphStoreFactory {
    private final HighAvailabilityServices highAvailabilityServices;

    public HaServicesJobGraphStoreFactory(HighAvailabilityServices highAvailabilityServices) {
        this.highAvailabilityServices = highAvailabilityServices;
    }

    @Override
    public JobGraphStore create() {
        try {
            return highAvailabilityServices.getJobGraphStore();
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Could not create %s from %s.",
                            JobGraphStore.class.getSimpleName(),
                            highAvailabilityServices.getClass().getSimpleName()),
                    e);
        }
    }
}
