package org.apache.flink.metrics.prometheus;

import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Config options of PrometheusReporter. */
@Documentation.SuffixOption(ConfigConstants.METRICS_REPORTER_PREFIX + "prometheus")
public class PrometheusReporterOptions {
    public static final ConfigOption<Boolean> ENABLE_HISTOGRAM_MAX =
            ConfigOptions.key("enable_histogram_max")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Specifies whether to enable maximum from histogram or not. If enabled, "
                                    + "the histogram metrics will include quantile=1.0 which is equivalent to max.");

    public static final ConfigOption<Boolean> ENABLE_HISTOGRAM_MIN =
            ConfigOptions.key("enable_histogram_min")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Specifies whether to enable minimum from histogram or not. If enabled, "
                                    + "the histogram metrics will include quantile=0.0 which is equivalent to min.");
}
