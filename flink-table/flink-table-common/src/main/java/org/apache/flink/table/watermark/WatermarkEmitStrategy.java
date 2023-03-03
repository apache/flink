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

package org.apache.flink.table.watermark;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.PipelineOptions;

/** The strategy for emitting watermark. */
@PublicEvolving
public enum WatermarkEmitStrategy {
    /** Emit watermark for every event. */
    ON_EVENT("on-event"),

    /**
     * Emit watermark periodically. the period is configured by {@link
     * PipelineOptions#AUTO_WATERMARK_INTERVAL}
     */
    ON_PERIODIC("on-periodic"),
    ;

    private final String alias;

    public String getAlias() {
        return alias;
    }

    WatermarkEmitStrategy(String alias) {
        this.alias = alias;
    }

    public boolean isOnEvent() {
        return this == ON_EVENT;
    }

    public boolean isOnPeriodic() {
        return this == ON_PERIODIC;
    }

    @Override
    public String toString() {
        return this.alias;
    }
}
