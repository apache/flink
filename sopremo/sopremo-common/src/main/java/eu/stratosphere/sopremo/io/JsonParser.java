package eu.stratosphere.sopremo.io;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

import eu.stratosphere.nephele.fs.FSDataInputStream;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.ObjectMapper;

public class JsonParser {

	public class JsonParseException extends JsonProcessingException {

		/**
		 * 
		 */
		private static final long serialVersionUID = -200084994943556971L;

	}

	public class JsonProcessingException extends IOException {

		/**
		 * 
		 */
		private static final long serialVersionUID = -794115145360667900L;

	}

	public enum JsonToken {
		START_ARRAY, END_ARRAY

	}

	public JsonParser(FSDataInputStream stream) {
	}

	public JsonParser(InputStreamReader inputStreamReader) {
	}

	public JsonParser(URL url) {
	}

	public JsonParser(String value) {
	}

	public JsonNode readValueAsTree() {
		return null;
	}

	public Object nextToken() {
		return null;
	}

	public void setCodec(ObjectMapper objectMapper) {
	}

	public void close() {
	}

	public void clearCurrentToken() {
	}

}
