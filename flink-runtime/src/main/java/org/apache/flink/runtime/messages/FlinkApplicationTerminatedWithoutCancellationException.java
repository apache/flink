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

package org.apache.flink.runtime.messages;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

/**
 * Exception indicating that the Flink application with the given application ID has terminated
 * without cancellation.
 */
public class FlinkApplicationTerminatedWithoutCancellationException extends FlinkException {

    private static final long serialVersionUID = 2294698055059659024L;

    private final ApplicationState status;

    public FlinkApplicationTerminatedWithoutCancellationException(
            ApplicationID applicationId, ApplicationState status) {
        super(
                String.format(
                        "Flink application (%s) was not canceled, but instead %s.",
                        applicationId, assertNotCanceled(status)));
        this.status = status;
    }

    public ApplicationState getApplicationStatus() {
        return status;
    }

    private static ApplicationState assertNotCanceled(ApplicationState status) {
        Preconditions.checkState(status != ApplicationState.CANCELED);
        return status;
    }
}
