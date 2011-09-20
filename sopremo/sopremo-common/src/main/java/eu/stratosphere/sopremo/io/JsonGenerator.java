package eu.stratosphere.sopremo.io;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;

import eu.stratosphere.nephele.fs.FSDataOutputStream;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;

public class JsonGenerator {

	BufferedWriter writer;
	
	public JsonGenerator(FSDataOutputStream stream) {
		this.writer = new BufferedWriter(new OutputStreamWriter(stream));
	}

	public JsonGenerator(StringWriter writer) {
		this.writer = new BufferedWriter(writer);
	}

	public JsonGenerator(File file) throws IOException {
		this.writer = new BufferedWriter(new FileWriter(file));
	}

	public void close() throws IOException {
		this.writer.close();
	}

	public void writeTree(JsonNode value) throws IOException {
		if(value!=null){
			this.writer.write(value.toString());
			this.writer.flush();
		}
	}

	public void writeEndArray() throws IOException {
		JsonToken.END_ARRAY.write(this.writer);
		
	}

	public void writeStartArray() throws IOException {
		JsonToken.START_ARRAY.write(this.writer);
	}

}
