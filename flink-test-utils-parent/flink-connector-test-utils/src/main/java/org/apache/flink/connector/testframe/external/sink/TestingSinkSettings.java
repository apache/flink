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

package org.apache.flink.connector.testframe.external.sink;

import org.apache.flink.core.execution.CheckpointingMode;

import static org.apache.flink.streaming.api.CheckpointingMode.convertFromCheckpointingMode;
import static org.apache.flink.streaming.api.CheckpointingMode.convertToCheckpointingMode;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Settings for configuring the sink under testing. */
public class TestingSinkSettings {
    private final CheckpointingMode checkpointingMode;

    public static Builder builder() {
        return new Builder();
    }

    private TestingSinkSettings(CheckpointingMode checkpointingMode) {
        this.checkpointingMode = checkpointingMode;
    }

    /**
     * Checkpointing mode required for the sink. This method is required for downstream projects
     * e.g. Flink connectors extending this test for the case when there should be supported Flink
     * versions below 1.20. Could be removed together with dropping support for Flink 1.19.
     */
    @Deprecated
    public org.apache.flink.streaming.api.CheckpointingMode getCheckpointingMode() {
        return convertFromCheckpointingMode(checkpointingMode);
    }

    /** Builder class for {@link TestingSinkSettings}. */
    public static class Builder {
        private CheckpointingMode checkpointingMode;

        public Builder setCheckpointingMode(CheckpointingMode checkpointingMode) {
            this.checkpointingMode = checkpointingMode;
            return this;
        }

        /**
         * This method is required for downstream projects e.g. Flink connectors extending this test
         * for the case when there should be supported Flink versions below 1.20. Could be removed
         * together with dropping support for Flink 1.19.
         */
        @Deprecated
        public Builder setCheckpointingMode(
                org.apache.flink.streaming.api.CheckpointingMode checkpointingMode) {
            this.checkpointingMode = convertToCheckpointingMode(checkpointingMode);
            return this;
        }

        public TestingSinkSettings build() {
            sanityCheck();
            return new TestingSinkSettings(checkpointingMode);
        }

        private void sanityCheck() {
            checkNotNull(checkpointingMode, "Checkpointing mode is not specified");
        }
    }
}
