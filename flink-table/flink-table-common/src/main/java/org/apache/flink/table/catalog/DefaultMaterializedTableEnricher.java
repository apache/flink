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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.CatalogMaterializedTable.LogicalRefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;

import java.time.Duration;

/**
 * Default implementation of {@link MaterializedTableEnricher}.
 *
 * <p>Applies default freshness values based on refresh mode and determines the physical refresh
 * mode using freshness threshold comparison.
 */
@Internal
public class DefaultMaterializedTableEnricher implements MaterializedTableEnricher {

    private final IntervalFreshness defaultContinuousFreshness;
    private final IntervalFreshness defaultFullFreshness;
    private final Duration freshnessThreshold;

    public static DefaultMaterializedTableEnricher create(
            final Duration defaultContinuousFreshness,
            final Duration defaultFullFreshness,
            final Duration freshnessThreshold) {
        final IntervalFreshness continuousFreshness =
                IntervalFreshness.fromDuration(defaultContinuousFreshness);
        final IntervalFreshness fullFreshness =
                IntervalFreshness.fromDuration(defaultFullFreshness);
        return new DefaultMaterializedTableEnricher(
                continuousFreshness, fullFreshness, freshnessThreshold);
    }

    private DefaultMaterializedTableEnricher(
            final IntervalFreshness defaultContinuousFreshness,
            final IntervalFreshness defaultFullFreshness,
            final Duration freshnessThreshold) {
        this.defaultContinuousFreshness = defaultContinuousFreshness;
        this.defaultFullFreshness = defaultFullFreshness;
        this.freshnessThreshold = freshnessThreshold;
    }

    @Override
    public MaterializedTableEnrichmentResult enrich(final CatalogMaterializedTable table) {
        // Determine the final freshness value
        final IntervalFreshness finalFreshness = deriveFreshness(table);

        // Derive the final refresh mode using the freshness and threshold
        final RefreshMode finalRefreshMode =
                deriveRefreshMode(
                        table.getLogicalRefreshMode(), finalFreshness, freshnessThreshold);

        return new MaterializedTableEnrichmentResult(finalFreshness, finalRefreshMode);
    }

    /**
     * Returns user-specified freshness or applies mode-specific defaults: FULL mode uses {@code
     * defaultFullFreshness}, others use {@code defaultContinuousFreshness}.
     */
    private IntervalFreshness deriveFreshness(final CatalogMaterializedTable table) {
        final IntervalFreshness finalFreshness;
        if (table.getDefinitionFreshness() != null) {
            // User provided an explicit freshness, use it
            finalFreshness = table.getDefinitionFreshness();
        } else {
            // User omitted freshness, choose default based on logical mode
            if (table.getLogicalRefreshMode() == LogicalRefreshMode.FULL) {
                finalFreshness = defaultFullFreshness;
            } else {
                // For AUTOMATIC or CONTINUOUS modes, use the continuous default
                finalFreshness = defaultContinuousFreshness;
            }
        }
        return finalFreshness;
    }

    /**
     * Determines physical refresh mode: CONTINUOUS if freshness is below threshold or explicitly
     * requested, otherwise FULL (validated for cron conversion).
     */
    public RefreshMode deriveRefreshMode(
            LogicalRefreshMode logicalRefreshMode,
            IntervalFreshness freshness,
            Duration threshold) {
        if (logicalRefreshMode != LogicalRefreshMode.FULL) {
            final Duration definedFreshness = freshness.toDuration();
            if (logicalRefreshMode == LogicalRefreshMode.CONTINUOUS
                    || definedFreshness.compareTo(threshold) < 0) {
                return RefreshMode.CONTINUOUS;
            }
        }

        // Validate that freshness can be converted to cron for FULL mode
        IntervalFreshness.validateFreshnessForCron(freshness);
        return RefreshMode.FULL;
    }
}
