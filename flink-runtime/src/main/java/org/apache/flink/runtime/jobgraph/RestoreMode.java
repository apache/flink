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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.InlineElement;

import static org.apache.flink.configuration.description.TextElement.text;

/** Defines how Flink should restore from a given savepoint or retained checkpoint. */
@PublicEvolving
public enum RestoreMode implements DescribedEnum {
    CLAIM(
            "Flink will take ownership of the given snapshot. It will clean the"
                    + " snapshot once it is subsumed by newer ones."),
    LEGACY(
            "Flink will not claim ownership of the snapshot and will not delete the files. However, "
                    + "it can directly depend on the existence of the files of the restored checkpoint. "
                    + "It might not be safe to delete checkpoints that were restored in legacy mode ");

    private final String description;

    RestoreMode(String description) {
        this.description = description;
    }

    @Override
    @Internal
    public InlineElement getDescription() {
        return text(description);
    }
}
