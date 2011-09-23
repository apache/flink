package eu.stratosphere.sopremo.io;

import it.unimi.dsi.fastutil.Stack;
import it.unimi.dsi.fastutil.chars.Char2ObjectMap;
import it.unimi.dsi.fastutil.chars.Char2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.nephele.fs.FSDataInputStream;
import eu.stratosphere.sopremo.jsondatamodel.ArrayNode;
import eu.stratosphere.sopremo.jsondatamodel.BigIntegerNode;
import eu.stratosphere.sopremo.jsondatamodel.BooleanNode;
import eu.stratosphere.sopremo.jsondatamodel.DecimalNode;
import eu.stratosphere.sopremo.jsondatamodel.IntNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.LongNode;
import eu.stratosphere.sopremo.jsondatamodel.NullNode;
import eu.stratosphere.sopremo.jsondatamodel.ObjectNode;
import eu.stratosphere.sopremo.jsondatamodel.TextNode;

public class JsonParser {

	private BufferedReader reader;
	
	private Stack<JsonNode> state = new ObjectArrayList<JsonNode>();

	ContainerNode root = new ContainerNode();

	private boolean insideString = false;

	private boolean wasString = false;
	
	private boolean isArray = false;

	private Char2ObjectMap<CharacterHandler> handler = new Char2ObjectOpenHashMap<CharacterHandler>(
		new char[] { '[', ']', '{', '}', ':', '\"', ',', ' ' },
		new CharacterHandler[] { new OpenArrayHandler(), new CloseArrayHandler(), new OpenObjectHandler(),
			new CloseObjectHandler(), new KeyValueSeperatorHandler(), new StringHandler(),
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
		int nextChar = this.reader.read();

		do {
			char character = (char) nextChar;
			if (insideString && character != '\"') {
				sb.append(character);

			} else {
				this.handler.get(character).handleCharacter(sb, character);
			}
			nextChar = this.reader.read();

		} while ((this.state.top() != root || nextChar != ',')  && (nextChar) != -1);

		if (sb.length() != 0) {
			if(root == state.top())
			root.addValue(JsonParser.parsePrimitive(sb.toString()));
			else return JsonParser.parsePrimitive(sb.toString());
		}

		if(isArray){
			return root.remove(0);
		} else return ((ArrayNode) root.build()).get(0);
		
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
		public void handleCharacter(StringBuilder sb, char character) throws JsonParseException;
	}

	private class OpenArrayHandler implements CharacterHandler {

		@Override
		public void handleCharacter(StringBuilder sb, char character) {
			if (root == state.top() && !isArray) {				
				isArray = true;
			} else {
				state.push(new ContainerNode());
			}
		}

	}

	private class OpenObjectHandler implements CharacterHandler {
	
		@Override
		public void handleCharacter(StringBuilder sb, char character) {
			ContainerNode node = new ContainerNode();
			state.push(node);
	
		}
	
	}

	private class CloseArrayHandler implements CharacterHandler {

		@Override
		public void handleCharacter(StringBuilder sb, char character) throws JsonParseException {
			ContainerNode node;
			if (state.top() != root) {
				node = (ContainerNode) state.pop();
			} else {
				node = (ContainerNode) state.top();
			}

			if (sb.length() != 0) {
				if (!wasString) {
					node.addValue(JsonParser.parsePrimitive(sb.toString()));
				} else {
					node.addValue(TextNode.valueOf(sb.toString()));
					wasString = false;
				}
				sb.setLength(0);
			}

			((ContainerNode) state.top()).addValue(node.build());

		}
	}

	private class CloseObjectHandler implements CharacterHandler {
	
		@Override
		public void handleCharacter(StringBuilder sb, char character) throws JsonParseException {
			ContainerNode node;
			if (state.top() != root) {
				node = (ContainerNode) state.pop();
			} else {
				node = (ContainerNode) state.top();
			}
			if (sb.length() != 0) {
				if (!wasString) {
					node.addValue(JsonParser.parsePrimitive(sb.toString()));
				} else {
					node.addValue(TextNode.valueOf(sb.toString()));
					wasString = false;
				}
				sb.setLength(0);
			}
	
			((ContainerNode) state.top()).addValue(node.build());
				
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
					wasString = true;
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
				ContainerNode node = (ContainerNode) state.top();
				if (!wasString) {
					node.addValue(JsonParser.parsePrimitive(sb.toString()));
				} else {
					node.addValue(TextNode.valueOf(sb.toString()));
					wasString = false;
				}
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

	private class KeyValueSeperatorHandler implements CharacterHandler {

		@Override
		public void handleCharacter(StringBuilder sb, char character) throws JsonParseException {
			if (sb.length() != 0) {
				ContainerNode node = (ContainerNode) state.top();
				node.addKey(sb);
				wasString = false;
				sb.setLength(0);
			}
		}

	}

	private class DefaultHandler implements CharacterHandler {

		@Override
		public void handleCharacter(StringBuilder sb, char character) throws JsonParseException {

			if (Character.isWhitespace(character)) {
				handler.get(' ').handleCharacter(sb, character);
			} else {
				sb.append(character);
			}

		}
	}

	private class ContainerNode extends JsonNode {

		/**
		 * 
		 */
		private static final long serialVersionUID = -7285733826083281420L;

		private List<String> keys = new ArrayList<String>();

		private List<JsonNode> values = new ArrayList<JsonNode>();

		public void addKey(StringBuilder sb) {
			this.keys.add(sb.toString());
		}

		public void addValue(JsonNode node) {
			this.values.add(node);
		}

		public JsonNode remove(int index) throws JsonParseException{
			if(this.keys.isEmpty()){
				return this.values.remove(index);
			}
			throw new JsonParseException();
		}
		
		public JsonNode build() throws JsonParseException {
			JsonNode node;

			if (this.keys.size() == 0) {
				// this ContainerNode represents an ArrayNode
				node = new ArrayNode();
				for (JsonNode value : this.values) {
					((ArrayNode) node).add(value);
				}

			} else {
				// this ContainerNode represents an ObjectNode

				if (this.keys.size() != this.values.size()) {
					throw new JsonParseException();
				}

				node = new ObjectNode();
				for (int i = 0; i < this.keys.size(); i++) {
					((ObjectNode) node).put(this.keys.get(i), this.values.get(i));
				}
			}

			return node;
		}

		@Override
		public boolean equals(Object o) {
			return false;
		}

		@Override
		public int getTypePos() {
			return 0;
		}

		@Override
		public TYPES getType() {
			return null;
		}

		@Override
		public void read(DataInput in) throws IOException {
		}

		@Override
		public void write(DataOutput out) throws IOException {
		}

		@Override
		public StringBuilder toString(StringBuilder sb) {
			return sb;
		}

	}
}
