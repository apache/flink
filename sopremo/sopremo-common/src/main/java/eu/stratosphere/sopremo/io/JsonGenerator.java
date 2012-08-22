package eu.stratosphere.sopremo.io;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * Writes the string-representation of {@link IJsonNode}s to a specified sink.
 */
public class JsonGenerator {

	BufferedWriter writer;

	boolean isFirst = true;

	/**
	 * Initializes a JsonGenerator which uses the given {@link OutputStream} as a sink.
	 * 
	 * @param stream
	 *        the stream that should be used as a sink
	 */
	public JsonGenerator(final OutputStream stream) {
		this.writer = new BufferedWriter(new OutputStreamWriter(stream));
	}

	/**
	 * Initializes a JsonGenerator which uses the given {@link Writer} as a sink.
	 * 
	 * @param writer
	 *        the writer that should be used as a sink
	 */
	public JsonGenerator(final Writer writer) {
		this.writer = new BufferedWriter(writer);
	}

	/**
	 * Initializes a JsonGenerator which uses the given {@link File} as a sink.
	 * 
	 * @param file
	 *        the file that should be used as a sink
	 * @throws IOException
	 */
	public JsonGenerator(final File file) throws IOException {
		this.writer = new BufferedWriter(new FileWriter(file));
	}

	/**
	 * Closes the connection to the specified sink.
	 * 
	 * @throws IOException
	 */
	public void close() throws IOException {
		this.writer.close();
	}

	/**
	 * Writes the given {@link IJsonNode} to the specified sink. The string-representations of multiple invocations are
	 * separated by a comma.
	 * 
	 * @param iJsonNode
	 *        the node that should be written to the sink
	 * @throws IOException
	 */
	public void writeTree(final IJsonNode iJsonNode) throws IOException {
		if (iJsonNode != null) {
			if (!this.isFirst)
				this.writer.write(",");
			this.writer.write(iJsonNode.toString());
			this.writer.flush();
			this.isFirst = false;
		}
	}

	/**
	 * Writes the end-array-token to the specified sink. The token is specified in {@link JsonToken#END_ARRAY}.
	 * 
	 * @throws IOException
	 */
	public void writeEndArray() throws IOException {
		JsonToken.END_ARRAY.write(this.writer);
		this.writer.flush();

	}

	/**
	 * Writes the start-array-token to the specified sink. The token is specified in {@link JsonToken#START_ARRAY}.
	 * 
	 * @throws IOException
	 */
	public void writeStartArray() throws IOException {
		JsonToken.START_ARRAY.write(this.writer);
		this.writer.flush();
	}

}
