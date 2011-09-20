package eu.stratosphere.sopremo.io;

import java.io.File;
import java.io.StringWriter;
import java.nio.charset.Charset;

import eu.stratosphere.nephele.fs.FSDataOutputStream;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;

public class JsonGenerator {

	public JsonGenerator(FSDataOutputStream stream, Charset encoding) {
	}

	public JsonGenerator(StringWriter writer) {
	}

	public JsonGenerator(File tempFile) {
	}

	public void close() {
	}

	public void writeTree(JsonNode adhocValues) {
	}

	public void writeEndArray() {
	}

	public void writeStartArray() {
	}

}
