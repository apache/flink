package eu.stratosphere.sopremo.io;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import eu.stratosphere.sopremo.type.IJsonNode;

public class JsonGenerator {

	BufferedWriter writer;

	boolean isFirst = true;

	public JsonGenerator(final OutputStream stream) {
		this.writer = new BufferedWriter(new OutputStreamWriter(stream));
	}

	public JsonGenerator(final Writer writer) {
		this.writer = new BufferedWriter(writer);
	}

	public JsonGenerator(final File file) throws IOException {
		this.writer = new BufferedWriter(new FileWriter(file));
	}

	public void close() throws IOException {
		this.writer.close();
	}

	public void writeTree(final IJsonNode iJsonNode) throws IOException {
		if (iJsonNode != null) {
			if (!this.isFirst)
				this.writer.write(",");
			this.writer.write(iJsonNode.toString());
			this.writer.flush();
			this.isFirst = false;
		}
	}

	public void writeEndArray() throws IOException {
		JsonToken.END_ARRAY.write(this.writer);
		this.writer.flush();

	}

	public void writeStartArray() throws IOException {
		JsonToken.START_ARRAY.write(this.writer);
		this.writer.flush();
	}

}
