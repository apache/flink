package org.apache.flink.streaming.connectors.gcp.pubsub.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Options for PubSub tables supported by the {@code CREATE TABLE ... WITH ...} clause of the Flink
 * SQL dialect and the Flink Table API.
 */
@PublicEvolving
public class PubSubConnectorConfigOptions {

    public static final ConfigOption<String> PROJECT_NAME =
            ConfigOptions.key("projectName")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Name of the PubSub project backing this table.");

    public static final ConfigOption<String> TOPIC =
            ConfigOptions.key("topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Name of the PubSub topic backing this table.");

    public static final String IDENTIFIER = "pubsub";

    public static final String JSON_FORMAT_PROPERTIES_PREFIX = "json.";

    /**
     * Prefixes of properties that are validated by downstream components and should not be
     * validated by the Table API infrastructure.
     */
    public static final String[] NON_VALIDATED_PREFIXES =
            new String[] {JSON_FORMAT_PROPERTIES_PREFIX};
}
