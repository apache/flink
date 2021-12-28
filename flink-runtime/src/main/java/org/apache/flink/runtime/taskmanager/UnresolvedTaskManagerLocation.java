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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.runtime.clusterframework.types.ResourceID;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class encapsulates the connection information of a TaskManager, without resolving the
 * hostname. See also {@link TaskManagerLocation}.
 */
public class UnresolvedTaskManagerLocation implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ResourceID resourceID;
    private final String externalAddress;

    /** Data port of the configured default shuffle service. */
    private final int defaultDataPort;

    /** Mapping from shuffle service factory name to external data port. */
    private final Map<String, Integer> dataPortByFactoryName;

    public UnresolvedTaskManagerLocation(
            final ResourceID resourceID,
            final String externalAddress,
            int defaultDataPort,
            final Map<String, Integer> dataPortByFactoryName) {
        this.defaultDataPort = defaultDataPort;
        this.dataPortByFactoryName = checkNotNull(dataPortByFactoryName);
        // this check includes the default data port
        checkDataPorts(dataPortByFactoryName.values());

        this.resourceID = checkNotNull(resourceID);
        this.externalAddress = checkNotNull(externalAddress);
    }

    public ResourceID getResourceID() {
        return resourceID;
    }

    public String getExternalAddress() {
        return externalAddress;
    }

    public Map<String, Integer> allShuffleDataPorts() {
        return dataPortByFactoryName;
    }

    public int defaultShuffleDataPort() {
        return defaultDataPort;
    }

    private static void checkDataPorts(Collection<Integer> dataPorts) {
        for (int dataPort : dataPorts) {
            // -1 indicates a local instance connection info
            checkArgument(dataPort > 0 || dataPort == -1, "dataPort must be > 0, or -1 (local)");
        }
    }
}
