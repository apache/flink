package org.apache.flink.metrics.datadog.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SerializationUtils {
	private static final ObjectMapper MAPPER = new ObjectMapper();

	public static String serialize(Object obj) throws JsonProcessingException {
		return MAPPER.writeValueAsString(obj);
	}
}
