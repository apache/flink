package eu.stratosphere.sopremo.io;

import java.io.IOException;
import java.io.Writer;

public enum JsonToken {
	START_ARRAY('['),
	END_ARRAY(']'),
	START_OBJECT('{'),
	END_OBJECT('}'),
	KEY_VALUE_DELIMITER(':'),
	START_STRING('\"'),
	WHITE_SPACE(' ');

	private char token;

	private JsonToken(final char token) {
		this.token = token;
	}

	public void write(final Writer writer) throws IOException {
		writer.write(this.token);
	}

}