package org.apache.flink.streaming.api.environment;

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

public class PathTrackerOptions {

    public static final ConfigOption<Boolean> ENABLE = key("pathtracker.enable")
            .booleanType()
            .defaultValue(false)
            .withDescription("Enable path tracker features");
}
