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

package org.apache.flink.connector.testframe.external.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.CheckpointingMode;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Settings for configuring the source under testing. */
public class TestingSourceSettings {
    private final Boundedness boundedness;
    private final CheckpointingMode checkpointingMode;

    public static Builder builder() {
        return new Builder();
    }

    /** Boundedness of the source. */
    public Boundedness getBoundedness() {
        return boundedness;
    }

    /** Checkpointing mode required for the source. */
    public CheckpointingMode getCheckpointingMode() {
        return checkpointingMode;
    }

    private TestingSourceSettings(Boundedness boundedness, CheckpointingMode checkpointingMode) {
        this.boundedness = boundedness;
        this.checkpointingMode = checkpointingMode;
    }

    /** Builder class for {@link TestingSourceSettings}. */
    public static class Builder {
        private Boundedness boundedness;
        private CheckpointingMode checkpointingMode = CheckpointingMode.EXACTLY_ONCE;

        public Builder setBoundedness(Boundedness boundedness) {
            this.boundedness = boundedness;
            return this;
        }

        public Builder setCheckpointingMode(CheckpointingMode checkpointingMode) {
            this.checkpointingMode = checkpointingMode;
            return this;
        }

        public TestingSourceSettings build() {
            sanityCheck();
            return new TestingSourceSettings(boundedness, checkpointingMode);
        }

        private void sanityCheck() {
            checkNotNull(boundedness, "Boundedness is not specified");
            checkNotNull(checkpointingMode, "Checkpointing mode is not specified");
        }
    }
}
