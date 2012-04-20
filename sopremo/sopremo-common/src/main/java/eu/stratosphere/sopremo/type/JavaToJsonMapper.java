package eu.stratosphere.sopremo.type;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.util.reflect.ReflectUtil;

public class JavaToJsonMapper {
	public static final JavaToJsonMapper INSTANCE = new JavaToJsonMapper();

	private final HashMap<Class<? extends Object>, Constructor<? extends JsonNode>> typeDict = new HashMap<Class<? extends Object>, Constructor<? extends JsonNode>>();

	public JavaToJsonMapper() {
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

	@SuppressWarnings("unchecked")
	public Class<? extends IJsonNode> classToJsonType(final Class<?> javaClass) {
		if (IJsonNode.class.isAssignableFrom(javaClass))
			return (Class<? extends IJsonNode>) javaClass;

		if (javaClass == Void.TYPE)
			return NullNode.class;

		if (CharSequence.class.isAssignableFrom(javaClass))
			return TextNode.class;

		if (javaClass.isArray() || Collection.class.isAssignableFrom(javaClass))
			return ArrayNode.class;

		if (Map.class.isAssignableFrom(javaClass))
			return ObjectNode.class;

		if (ReflectUtil.isSameTypeOrPrimitive(javaClass, Boolean.class))
			return BooleanNode.class;

		return this.typeDict.get(ReflectUtil.getClassForPrimtive(javaClass)).getDeclaringClass();
	}

	public JsonNode valueToTree(final Object value) {
		if (value == null)
			return NullNode.getInstance();

		final Class<? extends Object> valueClass = value.getClass();
		if (value instanceof JsonNode)
			return (JsonNode) value;

		if (value instanceof CharSequence)
			return TextNode.valueOf(value.toString());

		if (valueClass.isArray()) {
			final ArrayNode arrayNode = new ArrayNode();
			final int length = Array.getLength(value);
			for (int i = 0; i < length; i++)
				arrayNode.add(this.valueToTree(Array.get(value, i)));
			return arrayNode;
		}

		if (Collection.class.isAssignableFrom(valueClass)) {
			final ArrayNode arrayNode = new ArrayNode();
			for (final Object element : (Collection<?>) value)
				arrayNode.add(this.valueToTree(element));
			return arrayNode;
		}

		if (Map.class.isAssignableFrom(valueClass)) {
			final ObjectNode objectNode = new ObjectNode();
			for (final Entry<?, ?> element : ((Map<?, ?>) value).entrySet())
				objectNode.put(element.getKey().toString(), this.valueToTree(element.getValue()));
			return objectNode;
		}

		if (value instanceof Boolean)
			return BooleanNode.valueOf((Boolean) value);

		try {
			return this.typeDict.get(valueClass).newInstance(value);
		} catch (final Exception e) {
			throw new IllegalArgumentException("Cannot map object " + value + " to json node", e);
		}

	}

}
