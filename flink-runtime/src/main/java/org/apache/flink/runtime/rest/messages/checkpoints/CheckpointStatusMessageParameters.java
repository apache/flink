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

package org.apache.flink.runtime.rest.messages.checkpoints;

import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/** The parameters for triggering a checkpoint. */
public class CheckpointStatusMessageParameters extends MessageParameters {

    public final JobIDPathParameter jobIdPathParameter = new JobIDPathParameter();

    public final TriggerIdPathParameter triggerIdPathParameter = new TriggerIdPathParameter();

    @Override
    public Collection<MessagePathParameter<?>> getPathParameters() {
        return Collections.unmodifiableCollection(
                Arrays.asList(jobIdPathParameter, triggerIdPathParameter));
    }

    @Override
    public Collection<MessageQueryParameter<?>> getQueryParameters() {
        return Collections.emptyList();
    }
}
