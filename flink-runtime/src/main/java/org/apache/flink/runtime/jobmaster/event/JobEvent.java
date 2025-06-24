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

package org.apache.flink.runtime.jobmaster.event;

import java.io.Serializable;

/** A class that represents an event that happens during the job execution. */
public interface JobEvent extends Serializable {

    /**
     * Retrieves the type id of this job event. The type id is a unique identifier based on the
     * class of the specific event object.
     *
     * @return An integer representing the unique type id of this job event.
     */
    default int getType() {
        return JobEvents.getTypeID(this.getClass());
    }
}
