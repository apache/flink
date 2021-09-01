package org.apache.flink.streaming.connectors.gcp.pubsub.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Options for PubSub tables supported by the {@code CREATE TABLE ... WITH ...} clause of the Flink
 * SQL dialect and the Flink Table API.
 */
@PublicEvolving
public class PubSubConnectorOptions {

    public static final ConfigOption<String> PROJECT_NAME =
            ConfigOptions.key("projectName")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Name of the PubSub project backing this table.");

    public static final ConfigOption<String> SUBSCRIPTION =
            ConfigOptions.key("subscription")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Name of the PubSub subscription backing this table.");

    public static final String IDENTIFIER = "pubsub";
}
