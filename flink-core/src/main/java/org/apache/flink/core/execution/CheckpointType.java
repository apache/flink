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

package org.apache.flink.core.execution;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.InlineElement;

import static org.apache.flink.configuration.description.TextElement.text;

/** Describes the type in which a checkpoint should be taken. */
@PublicEvolving
public enum CheckpointType implements DescribedEnum {
    CONFIGURED("The checkpoint type derived from the job config"),

    FULL("A checkpoint type that checkpoints the entire state, common for all state backends."),

    INCREMENTAL(
            "A checkpoint type that checkpoints only the difference between snapshots, specific for certain state backend.");

    private final InlineElement description;
    public static final CheckpointType DEFAULT = CheckpointType.CONFIGURED;

    CheckpointType(String description) {
        this.description = text(description);
    }

    @Override
    public InlineElement getDescription() {
        return description;
    }
}
