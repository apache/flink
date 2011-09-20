package eu.stratosphere.sopremo.io;

import static eu.stratosphere.sopremo.io.JsonToken.*;
import it.unimi.dsi.fastutil.Stack;
import it.unimi.dsi.fastutil.chars.Char2ObjectMap;
import it.unimi.dsi.fastutil.chars.Char2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URL;

import eu.stratosphere.nephele.fs.FSDataInputStream;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.ObjectMapper;

public class JsonParser {

	private BufferedReader reader;

	private Stack<JsonToken> state = new ObjectArrayList<JsonToken>();
	
	private Char2ObjectMap<JsonToken> tokens = new Char2ObjectOpenHashMap<JsonToken>(
		new char[] { '[', ']', '{', '}', ':', '\"' },
		new JsonToken[] { START_ARRAY, END_ARRAY, START_OBJECT, END_OBJECT, KEY_VALUE_DELIMITER, QUOTE });

	public JsonParser(FSDataInputStream stream) {
		this.reader = new BufferedReader(new InputStreamReader(stream));
	}

	public JsonParser(InputStreamReader inputStreamReader) {
		this.reader = new BufferedReader(inputStreamReader);
	}

	public JsonParser(URL url) throws IOException {
		this.reader = new BufferedReader(new InputStreamReader(url.openStream()));
	}

	public JsonParser(String value) {
		this.reader = new BufferedReader(new StringReader(value));
	}

	public JsonNode readValueAsTree() {
		return null;
	}

	public Object nextToken() {
		return null;
	}

	public void setCodec(ObjectMapper objectMapper) {
	}

	public void close() throws IOException {
		this.reader.close();
	}

	public void clearCurrentToken() {
	}

}
