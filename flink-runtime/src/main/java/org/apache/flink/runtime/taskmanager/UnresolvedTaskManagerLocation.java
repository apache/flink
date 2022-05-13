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
    private final int dataPort;

    public UnresolvedTaskManagerLocation(
            final ResourceID resourceID, final String externalAddress, final int dataPort) {
        // -1 indicates a local instance connection info
        checkArgument(dataPort > 0 || dataPort == -1, "dataPort must be > 0, or -1 (local)");

        this.resourceID = checkNotNull(resourceID);
        this.externalAddress = checkNotNull(externalAddress);
        this.dataPort = dataPort;
    }

    public ResourceID getResourceID() {
        return resourceID;
    }

    public String getExternalAddress() {
        return externalAddress;
    }

    public int getDataPort() {
        return dataPort;
    }
}
