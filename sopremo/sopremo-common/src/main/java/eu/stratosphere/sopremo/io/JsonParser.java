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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;

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

	private Char2ObjectMap<JsonToken> tokens = new Char2ObjectOpenHashMap<JsonToken>(
		new char[] { '[', ']', '{', '}', ':', '\"', ' ' },
		new JsonToken[] { START_ARRAY, END_ARRAY, START_OBJECT, END_OBJECT, KEY_VALUE_DELIMITER, START_STRING,
			WHITE_SPACE });

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

	public JsonNode readValueAsTree() throws IOException {
		boolean insideString = false;

		ArrayNode root = new ArrayNode();
		state.push(root);
		
		StringBuilder sb = new StringBuilder();
		int nextChar;
		
		while ((nextChar = this.reader.read()) != -1) {
			char character = (char) nextChar;
			if (insideString && character != '\"') {
				sb.append(character);

			} else {
				switch (character) {
				case '[': {
					if (state.top() == root) {
						ArrayNode node = new ArrayNode();
						root.add(node);
						state.push(node);
					} else {
						ArrayNode newArray = new ArrayNode();
						((ArrayNode) state.top()).add(newArray);
						state.push(newArray);
					}
					break;
				}
				case ']': {
					ArrayNode node = (ArrayNode) state.pop();
					if (sb.length() != 0){
						node.add(PrimitiveParser.parse(sb.toString()));
						sb.setLength(0);
					}
					break;
				}
				case '{': {
					break;
				}
				case '}': {
					break;
				}
				case ':': {
					break;
				}
				case '\"': {
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
					break;
				}
				case ',': {
					if (sb.length() != 0) {
						ArrayNode node = (ArrayNode) state.top();
						node.add(PrimitiveParser.parse(sb.toString()));
						sb.setLength(0);
					}
					break;
				}
				
				// TODO check all whitespaces instead of ' '
				case ' ': {
					if (insideString) {
						sb.append(character);
					}
					break;
				}
				default: {
					sb.append(character);
					break;
				}
				}
			}

		}
		return root.isEmpty() ? PrimitiveParser.parse(sb.toString()) : root.get(0);
	}

	public Object nextToken() {
		return null;
	}

	public void close() throws IOException {
		this.reader.close();
	}

	public void clearCurrentToken() {
	}

	public static class PrimitiveParser {
		public static JsonNode parse(String value) {
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

		// private boolean isInt(String value) {
		// if (value.matches("^(\\+|-)?\\d+$")) {
		// // if(Long.valueOf(value) <= Integer.MAX_VALUE && Long.valueOf(value) >= Integer.MIN_VALUE){
		// // return true;
		// // }
		// try {
		// Integer.parseInt(value);
		// return true;
		// } catch (NumberFormatException e) {
		// return false;
		// }
		// }
		// return false;
		// }
		//
		// private boolean isLong(String value) {
		// if (value.matches("^(\\+|-)?\\d+$")) {
		// try {
		// Long.parseLong(value);
		// return true;
		// } catch (NumberFormatException e) {
		// return false;
		// }
		// }
		// return false;
		// }
	}
}
