package eu.stratosphere.sopremo.jsondatamodel;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;

public class ObjectMapper {

	private final HashMap<Class<? extends Object>, Constructor<? extends JsonNode>> typeDict = new HashMap<Class<? extends Object>, Constructor<? extends JsonNode>>();

	public ObjectMapper() {
		try {
			this.typeDict.put(Integer.class, IntNode.class.getConstructor(Integer.TYPE));
			this.typeDict.put(Long.class, LongNode.class.getConstructor(Long.TYPE));
			this.typeDict.put(BigInteger.class, BigIntegerNode.class.getConstructor(BigInteger.class));
			this.typeDict.put(BigDecimal.class, DecimalNode.class.getConstructor(BigDecimal.class));
			this.typeDict.put(Double.class, DoubleNode.class.getConstructor(Double.TYPE));
			this.typeDict.put(Float.class, DoubleNode.class.getConstructor(Float.TYPE));
			this.typeDict.put(String.class, TextNode.class.getConstructor(String.class));
		} catch (final SecurityException e) {
			e.printStackTrace();
		} catch (final NoSuchMethodException e) {
			e.printStackTrace();
		}

	}

	public JsonNode valueToTree(final Object value) {

		if (value == null)
			return NullNode.getInstance();
		final Class<? extends Object> valueClass = value.getClass();
		if (value instanceof JsonNode)
			return (JsonNode) value;
		if (value instanceof StringBuilder)
			return TextNode.valueOf(((StringBuilder) value).toString());
		if (valueClass.isArray()) {
			final ArrayNode arrayNode = new ArrayNode();
			final int length = Array.getLength(value);
			for (int i = 0; i < length; i++)
				arrayNode.add(this.valueToTree(Array.get(value, i)));
			return arrayNode;
		}
		if (value instanceof Boolean)
			return BooleanNode.valueOf((Boolean) value);
		try {
			return this.typeDict.get(valueClass).newInstance(value);
		} catch (final IllegalArgumentException e) {
			e.printStackTrace();
		} catch (final SecurityException e) {
			e.printStackTrace();
		} catch (final InstantiationException e) {
			e.printStackTrace();
		} catch (final IllegalAccessException e) {
			e.printStackTrace();
		} catch (final InvocationTargetException e) {
			e.printStackTrace();
		}
		return NullNode.getInstance();

	}

}
