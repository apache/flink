package org.apache.flink.streaming.connectors.elasticsearch;

import java.util.Map;

/**
 * transport client fatory for user to create different elasticsearch client such as xpack client.
 * @param <C> the client type.
 */
public interface TransportClientFactory<C> {
	C build(Map<String, String> userConfig);
}
