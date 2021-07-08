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

package org.apache.flink.runtime.query;

import org.apache.flink.api.common.JobID;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Arrays;

/**
 * Location information for all key groups of a {@link InternalKvState} instance.
 *
 * <p>This is populated by the {@link KvStateLocationRegistry} and used by the queryable state to
 * target queries.
 */
public class KvStateLocation implements Serializable {

    private static final long serialVersionUID = 1L;

    /** JobID the KvState instances belong to. */
    private final JobID jobId;

    /** JobVertexID the KvState instances belong to. */
    private final JobVertexID jobVertexId;

    /** Number of key groups of the operator the KvState instances belong to. */
    private final int numKeyGroups;

    /** Name under which the KvState instances have been registered. */
    private final String registrationName;

    /** IDs for each KvState instance where array index corresponds to key group index. */
    private final KvStateID[] kvStateIds;

    /**
     * Server address for each KvState instance where array index corresponds to key group index.
     */
    private final InetSocketAddress[] kvStateAddresses;

    /** Current number of registered key groups. */
    private int numRegisteredKeyGroups;

    /**
     * Creates the location information.
     *
     * @param jobId JobID the KvState instances belong to
     * @param jobVertexId JobVertexID the KvState instances belong to
     * @param numKeyGroups Number of key groups of the operator
     * @param registrationName Name under which the KvState instances have been registered
     */
    public KvStateLocation(
            JobID jobId, JobVertexID jobVertexId, int numKeyGroups, String registrationName) {
        this.jobId = Preconditions.checkNotNull(jobId, "JobID");
        this.jobVertexId = Preconditions.checkNotNull(jobVertexId, "JobVertexID");
        Preconditions.checkArgument(numKeyGroups >= 0, "Negative number of key groups");
        this.numKeyGroups = numKeyGroups;
        this.registrationName = Preconditions.checkNotNull(registrationName, "Registration name");
        this.kvStateIds = new KvStateID[numKeyGroups];
        this.kvStateAddresses = new InetSocketAddress[numKeyGroups];
    }

    /**
     * Returns the JobID the KvState instances belong to.
     *
     * @return JobID the KvState instances belong to
     */
    public JobID getJobId() {
        return jobId;
    }

    /**
     * Returns the JobVertexID the KvState instances belong to.
     *
     * @return JobVertexID the KvState instances belong to
     */
    public JobVertexID getJobVertexId() {
        return jobVertexId;
    }

    /**
     * Returns the number of key groups of the operator the KvState instances belong to.
     *
     * @return Number of key groups of the operator the KvState instances belong to
     */
    public int getNumKeyGroups() {
        return numKeyGroups;
    }

    /**
     * Returns the name under which the KvState instances have been registered.
     *
     * @return Name under which the KvState instances have been registered.
     */
    public String getRegistrationName() {
        return registrationName;
    }

    /**
     * Returns the current number of registered key groups.
     *
     * @return Number of registered key groups.
     */
    public int getNumRegisteredKeyGroups() {
        return numRegisteredKeyGroups;
    }

    /**
     * Returns the registered KvStateID for the key group index or <code>null</code> if none is
     * registered yet.
     *
     * @param keyGroupIndex Key group index to get ID for.
     * @return KvStateID for the key group index or <code>null</code> if none is registered yet
     * @throws IndexOutOfBoundsException If key group index < 0 or >= Number of key groups
     */
    public KvStateID getKvStateID(int keyGroupIndex) {
        if (keyGroupIndex < 0 || keyGroupIndex >= numKeyGroups) {
            throw new IndexOutOfBoundsException("Key group index");
        }

        return kvStateIds[keyGroupIndex];
    }

    /**
     * Returns the registered server address for the key group index or <code>null</code> if none is
     * registered yet.
     *
     * @param keyGroupIndex Key group index to get server address for.
     * @return the server address for the key group index or <code>null</code> if none is registered
     *     yet
     * @throws IndexOutOfBoundsException If key group index < 0 or >= Number of key groups
     */
    public InetSocketAddress getKvStateServerAddress(int keyGroupIndex) {
        if (keyGroupIndex < 0 || keyGroupIndex >= numKeyGroups) {
            throw new IndexOutOfBoundsException("Key group index");
        }

        return kvStateAddresses[keyGroupIndex];
    }

    /**
     * Registers a KvState instance for the given key group index.
     *
     * @param keyGroupRange Key group range to register
     * @param kvStateId ID of the KvState instance at the key group index.
     * @param kvStateAddress Server address of the KvState instance at the key group index.
     * @throws IndexOutOfBoundsException If key group range start < 0 or key group range end >=
     *     Number of key groups
     */
    public void registerKvState(
            KeyGroupRange keyGroupRange, KvStateID kvStateId, InetSocketAddress kvStateAddress) {

        if (keyGroupRange.getStartKeyGroup() < 0
                || keyGroupRange.getEndKeyGroup() >= numKeyGroups) {
            throw new IndexOutOfBoundsException("Key group index");
        }

        for (int kgIdx = keyGroupRange.getStartKeyGroup();
                kgIdx <= keyGroupRange.getEndKeyGroup();
                ++kgIdx) {

            if (kvStateIds[kgIdx] == null && kvStateAddresses[kgIdx] == null) {
                numRegisteredKeyGroups++;
            }

            kvStateIds[kgIdx] = kvStateId;
            kvStateAddresses[kgIdx] = kvStateAddress;
        }
    }

    /**
     * Registers a KvState instance for the given key group index.
     *
     * @param keyGroupRange Key group range to unregister.
     * @throws IndexOutOfBoundsException If key group range start < 0 or key group range end >=
     *     Number of key groups
     * @throws IllegalArgumentException If no location information registered for a key group index
     *     in the range.
     */
    void unregisterKvState(KeyGroupRange keyGroupRange) {
        if (keyGroupRange.getStartKeyGroup() < 0
                || keyGroupRange.getEndKeyGroup() >= numKeyGroups) {
            throw new IndexOutOfBoundsException("Key group index");
        }

        for (int kgIdx = keyGroupRange.getStartKeyGroup();
                kgIdx <= keyGroupRange.getEndKeyGroup();
                ++kgIdx) {
            if (kvStateIds[kgIdx] == null || kvStateAddresses[kgIdx] == null) {
                throw new IllegalArgumentException(
                        "Not registered. Probably registration/unregistration race.");
            }

            numRegisteredKeyGroups--;

            kvStateIds[kgIdx] = null;
            kvStateAddresses[kgIdx] = null;
        }
    }

    @Override
    public String toString() {
        return "KvStateLocation{"
                + "jobId="
                + jobId
                + ", jobVertexId="
                + jobVertexId
                + ", parallelism="
                + numKeyGroups
                + ", kvStateIds="
                + Arrays.toString(kvStateIds)
                + ", kvStateAddresses="
                + Arrays.toString(kvStateAddresses)
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        KvStateLocation that = (KvStateLocation) o;

        if (numKeyGroups != that.numKeyGroups) {
            return false;
        }
        if (!jobId.equals(that.jobId)) {
            return false;
        }
        if (!jobVertexId.equals(that.jobVertexId)) {
            return false;
        }
        if (!registrationName.equals(that.registrationName)) {
            return false;
        }
        if (!Arrays.equals(kvStateIds, that.kvStateIds)) {
            return false;
        }

        return Arrays.equals(kvStateAddresses, that.kvStateAddresses);
    }

    @Override
    public int hashCode() {
        int result = jobId.hashCode();
        result = 31 * result + jobVertexId.hashCode();
        result = 31 * result + numKeyGroups;
        result = 31 * result + registrationName.hashCode();
        result = 31 * result + Arrays.hashCode(kvStateIds);
        result = 31 * result + Arrays.hashCode(kvStateAddresses);
        return result;
    }
}
