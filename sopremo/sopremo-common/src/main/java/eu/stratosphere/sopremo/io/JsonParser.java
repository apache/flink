package eu.stratosphere.sopremo.io;

import it.unimi.dsi.fastutil.Stack;
import it.unimi.dsi.fastutil.chars.Char2ObjectMap;
import it.unimi.dsi.fastutil.chars.Char2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.charset.Charset;

import eu.stratosphere.nephele.fs.FSDataInputStream;
import eu.stratosphere.sopremo.jsondatamodel.ArrayNode;
import eu.stratosphere.sopremo.jsondatamodel.BigIntegerNode;
import eu.stratosphere.sopremo.jsondatamodel.BooleanNode;
import eu.stratosphere.sopremo.jsondatamodel.DecimalNode;
import eu.stratosphere.sopremo.jsondatamodel.IntNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.LongNode;
import eu.stratosphere.sopremo.jsondatamodel.NullNode;
import eu.stratosphere.sopremo.jsondatamodel.TextNode;

public class JsonParser {

	private BufferedReader reader;

	private Stack<JsonNode> state = new ObjectArrayList<JsonNode>();

	ArrayNode root = new ArrayNode();

	boolean insideString = false;

	// private Char2ObjectMap<JsonToken> tokens = new Char2ObjectOpenHashMap<JsonToken>(
	// new char[] { '[', ']', '{', '}', ':', '\"', ' ' },
	// new JsonToken[] { START_ARRAY, END_ARRAY, START_OBJECT, END_OBJECT, KEY_VALUE_DELIMITER, START_STRING,
	// WHITE_SPACE });

	private Char2ObjectMap<CharacterHandler> handler = new Char2ObjectOpenHashMap<CharacterHandler>(
		new char[] { '[', ']', /* '{', '}', ':', */'\"', ',', ' ' },
		new CharacterHandler[] { new OpenArrayHandler(), new CloseArrayHandler(), new StringHandler(),
			new CommaHandler(), new WhiteSpaceHandler() });

	public JsonParser(FSDataInputStream stream) {
		this(new InputStreamReader(stream, Charset.forName("utf-8")));
	}

	public JsonParser(Reader inputStreamReader) {
		this.reader = new BufferedReader(inputStreamReader);
		this.handler.defaultReturnValue(new DefaultHandler());
	}

	public JsonParser(URL url) throws IOException {
		this(new BufferedReader(new InputStreamReader(url.openStream())));
	}

	public JsonParser(String value) {
		this(new BufferedReader(new StringReader(value)));
	}

	public JsonNode readValueAsTree() throws IOException {

		this.state.push(this.root);

		StringBuilder sb = new StringBuilder();
		int nextChar;

		while ((nextChar = this.reader.read()) != -1) {
			char character = (char) nextChar;
			if (insideString && character != '\"') {
				sb.append(character);

			} else {
				this.handler.get(character).handleCharacter(sb, character);
				// switch (character) {
				// case '[': {
				// if (state.top() == root) {
				// ArrayNode node = new ArrayNode();
				// root.add(node);
				// state.push(node);
				// } else {
				// ArrayNode newArray = new ArrayNode();
				// ((ArrayNode) state.top()).add(newArray);
				// state.push(newArray);
				// }
				// break;
				// }
				// case ']': {
				// ArrayNode node = (ArrayNode) state.pop();
				// if (sb.length() != 0) {
				// node.add(this.parsePrimitive(sb.toString()));
				// sb.setLength(0);
				// }
				// break;
				// }
				// case '{': {
				// break;
				// }
				// case '}': {
				// break;
				// }
				// case ':': {
				// break;
				// }
				// case '\"': {
				// if (sb.length() == 0) {
				// insideString = true;
				// } else {
				// if (!sb.toString().endsWith("\\")) {
				// insideString = false;
				// ArrayNode node = (ArrayNode) state.top();
				// node.add(TextNode.valueOf(sb.toString()));
				// sb.setLength(0);
				// } else {
				// sb.append(character);
				// }
				// }
				// break;
				// }
				// case ',': {
				// if (sb.length() != 0) {
				// ArrayNode node = (ArrayNode) state.top();
				// node.add(this.parsePrimitive(sb.toString()));
				// sb.setLength(0);
				// }
				// break;
				// }
				//
				// // TODO check all whitespaces instead of ' '
				// case ' ': {
				// if (insideString) {
				// sb.append(character);
				// }
				// break;
				// }
				// default: {
				// sb.append(character);
				// break;
				// }
				// }
			}

		}

		if (state.top() != root) {
			throw new JsonParseException();
		}
		return root.isEmpty() ? JsonParser.parsePrimitive(sb.toString()) : root.get(0);
	}

	public Object nextToken() {
		return null;
	}

	public void close() throws IOException {
		this.reader.close();
	}

	public void clearCurrentToken() {
	}

	private static JsonNode parsePrimitive(String value) {
		if (value.equals("null")) {
			return NullNode.getInstance();
		}
		if (value.equals("true")) {
			return BooleanNode.TRUE;
		}
		if (value.equals("false")) {
			return BooleanNode.FALSE;
		}
		if (value.matches("^[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$")) {
			BigDecimal bigDec = new BigDecimal(value);
			if (bigDec.scale() == 0) {
				BigInteger bigInt = bigDec.unscaledValue();
				if (bigInt.bitLength() <= 31) {
					return IntNode.valueOf(bigInt.intValue());
				}
				if (bigInt.bitLength() <= 63) {
					return LongNode.valueOf(bigInt.longValue());
				}
				return BigIntegerNode.valueOf(bigInt);
			} else {
				return DecimalNode.valueOf(bigDec);
			}
		}

		return TextNode.valueOf(value);
	}

	private interface CharacterHandler {
		public void handleCharacter(StringBuilder sb, char character);
	}

	private class OpenArrayHandler implements CharacterHandler {

		@Override
		public void handleCharacter(StringBuilder sb, char character) {
			if (state.top() == root) {
				ArrayNode node = new ArrayNode();
				root.add(node);
				state.push(node);
			} else {
				ArrayNode newArray = new ArrayNode();
				((ArrayNode) state.top()).add(newArray);
				state.push(newArray);
			}
		}

	}

	private class CloseArrayHandler implements CharacterHandler {

		@Override
		public void handleCharacter(StringBuilder sb, char character) {
			ArrayNode node = (ArrayNode) state.pop();
			if (sb.length() != 0) {
				node.add(JsonParser.parsePrimitive(sb.toString()));
				sb.setLength(0);
			}

		}
	}

	private class StringHandler implements CharacterHandler {

		@Override
		public void handleCharacter(StringBuilder sb, char character) {
			if (sb.length() == 0) {
				insideString = true;
			} else {
				if (!sb.toString().endsWith("\\")) {
					insideString = false;
					ArrayNode node = (ArrayNode) state.top();
					node.add(TextNode.valueOf(sb.toString()));
					sb.setLength(0);
				} else {
					sb.append(character);
				}
			}
		}
	}

	private class CommaHandler implements CharacterHandler {

		@Override
		public void handleCharacter(StringBuilder sb, char character) {
			if (sb.length() != 0) {
				ArrayNode node = (ArrayNode) state.top();
				node.add(JsonParser.parsePrimitive(sb.toString()));
				sb.setLength(0);
			}
		}
	}

	private class WhiteSpaceHandler implements CharacterHandler {

		@Override
		public void handleCharacter(StringBuilder sb, char character) {
			if (insideString) {
				sb.append(character);
			}
		}

	}

	private class DefaultHandler implements CharacterHandler {

		@Override
		public void handleCharacter(StringBuilder sb, char character) {
			sb.append(character);
		}
	}
}
