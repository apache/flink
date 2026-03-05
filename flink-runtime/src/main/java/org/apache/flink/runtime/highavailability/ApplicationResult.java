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

package org.apache.flink.runtime.highavailability;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * The result of an application execution. This class collects information about a globally
 * terminated application.
 */
public class ApplicationResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ApplicationID applicationId;
    private final ApplicationState applicationState;

    public ApplicationResult(ApplicationID applicationId, ApplicationState applicationState) {
        this.applicationId = Preconditions.checkNotNull(applicationId);
        this.applicationState = Preconditions.checkNotNull(applicationState);
    }

    public ApplicationID getApplicationId() {
        return applicationId;
    }

    public ApplicationState getApplicationState() {
        return applicationState;
    }

    @Override
    public String toString() {
        return "ApplicationResult{"
                + "applicationId="
                + applicationId
                + ", applicationState="
                + applicationState
                + '}';
    }
}
