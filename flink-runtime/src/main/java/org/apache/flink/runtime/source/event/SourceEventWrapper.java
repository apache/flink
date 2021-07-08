/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.runtime.source.event;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

/** A wrapper operator event that contains a custom defined operator event. */
public class SourceEventWrapper implements OperatorEvent {

    private static final long serialVersionUID = 1L;

    private final SourceEvent sourceEvent;

    public SourceEventWrapper(SourceEvent sourceEvent) {
        this.sourceEvent = sourceEvent;
    }

    /** @return The {@link SourceEvent} in this SourceEventWrapper. */
    public SourceEvent getSourceEvent() {
        return sourceEvent;
    }

    @Override
    public String toString() {
        return String.format("SourceEventWrapper[%s]", sourceEvent);
    }
}
