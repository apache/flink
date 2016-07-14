package org.apache.flink.mesos.cli;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.util.Map;

public class FlinkMesosSessionCli {

	private static final ObjectMapper mapper = new ObjectMapper();

	public static Configuration decodeDynamicProperties(String dynamicPropertiesEncoded) {
		try {
			Configuration configuration = new Configuration();
			if(dynamicPropertiesEncoded != null) {
				TypeReference<Map<String, String>> typeRef = new TypeReference<Map<String, String>>() {};
				Map<String,String> props = mapper.readValue(dynamicPropertiesEncoded, typeRef);
				for (Map.Entry<String, String> property : props.entrySet()) {
					configuration.setString(property.getKey(), property.getValue());
				}
			}
			return configuration;
		}
		catch(IOException ex) {
			throw new IllegalArgumentException("unreadable encoded properties", ex);
		}
	}

	public static String encodeDynamicProperties(Configuration configuration) {
		try {
			String dynamicPropertiesEncoded = mapper.writeValueAsString(configuration.toMap());
			return dynamicPropertiesEncoded;
		}
		catch (JsonProcessingException ex) {
			throw new IllegalArgumentException("unwritable properties", ex);
		}
	}
}
