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

package org.apache.flink.table.gateway.workflow;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.refresh.RefreshHandler;

import java.io.Serializable;
import java.util.Objects;

/** A {@link RefreshHandler} instance for embedded workflow scheduler. */
@PublicEvolving
public class EmbeddedRefreshHandler implements RefreshHandler, Serializable {

    private static final long serialVersionUID = 1L;

    private final String workflowName;
    private final String workflowGroup;

    public EmbeddedRefreshHandler(String workflowName, String workflowGroup) {
        this.workflowName = workflowName;
        this.workflowGroup = workflowGroup;
    }

    @Override
    public String asSummaryString() {
        return String.format(
                "{\n  workflowName: %s,\n  workflowGroup: %s\n}", workflowName, workflowGroup);
    }

    public String getWorkflowName() {
        return workflowName;
    }

    public String getWorkflowGroup() {
        return workflowGroup;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EmbeddedRefreshHandler that = (EmbeddedRefreshHandler) o;
        return Objects.equals(workflowName, that.workflowName)
                && Objects.equals(workflowGroup, that.workflowGroup);
    }

    @Override
    public int hashCode() {
        return Objects.hash(workflowName, workflowGroup);
    }

    @Override
    public String toString() {
        return "EmbeddedRefreshHandler{"
                + "workflowName='"
                + workflowName
                + '\''
                + ", workflowGroup='"
                + workflowGroup
                + '\''
                + '}';
    }
}
