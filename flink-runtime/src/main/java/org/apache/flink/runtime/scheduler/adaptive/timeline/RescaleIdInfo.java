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

package org.apache.flink.runtime.scheduler.adaptive.timeline;

import org.apache.flink.util.AbstractID;

import java.io.Serializable;
import java.util.Objects;

/** The class to represent the rescale id description in one resource requirements rescale. */
public class RescaleIdInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final AbstractID rescaleUuid;
    private final AbstractID resourceRequirementsId;
    private final long rescaleAttemptId;

    public RescaleIdInfo(AbstractID resourceRequirementsId, Long rescaleAttemptId) {
        this.resourceRequirementsId = resourceRequirementsId;
        this.rescaleAttemptId = rescaleAttemptId;
        this.rescaleUuid = new AbstractID();
    }

    public AbstractID getRescaleUuid() {
        return rescaleUuid;
    }

    public AbstractID getResourceRequirementsId() {
        return resourceRequirementsId;
    }

    public long getRescaleAttemptId() {
        return rescaleAttemptId;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RescaleIdInfo rescaleIdInfo = (RescaleIdInfo) o;
        return rescaleAttemptId == rescaleIdInfo.rescaleAttemptId
                && Objects.equals(rescaleUuid, rescaleIdInfo.rescaleUuid)
                && Objects.equals(resourceRequirementsId, rescaleIdInfo.resourceRequirementsId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rescaleUuid, resourceRequirementsId, rescaleAttemptId);
    }

    @Override
    public String toString() {
        return "IdEpoch{"
                + "rescaleUuid="
                + rescaleUuid
                + ", resourceRequirementsId="
                + resourceRequirementsId
                + ", rescaleAttemptId="
                + rescaleAttemptId
                + '}';
    }
}
