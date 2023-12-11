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

package org.apache.flink.runtime.highavailability.nonha;

import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.leaderservice.LeaderServices;
import org.apache.flink.runtime.persistentservice.PersistentServices;

import javax.annotation.concurrent.GuardedBy;

/**
 * Abstract base class for non high-availability services.
 *
 * <p>This class returns the standalone variants for the checkpoint recovery factory, the submitted
 * job graph store, the running jobs registry and the blob store.
 */
public class NonHaServices implements HighAvailabilityServices {
    protected final Object lock = new Object();

    private final LeaderServices leaderServices;

    private final PersistentServices persistentServices;

    @GuardedBy("lock")
    private boolean shutdown;

    public NonHaServices(LeaderServices leaderServices, PersistentServices persistentServices) {
        this.leaderServices = leaderServices;
        this.persistentServices = persistentServices;

        shutdown = false;
    }

    // ----------------------------------------------------------------------
    // HighAvailabilityServices method implementations
    // ----------------------------------------------------------------------

    @Override
    public LeaderServices getLeaderServices() {
        return leaderServices;
    }

    @Override
    public PersistentServices getPersistentServices() {
        return persistentServices;
    }

    @Override
    public void close() throws Exception {
        synchronized (lock) {
            if (!shutdown) {
                persistentServices.close();
                leaderServices.close();
                shutdown = true;
            }
        }
    }

    @Override
    public void cleanupAllData() throws Exception {
        // this stores no data, do nothing here
    }
}
