package eu.stratosphere.sopremo.jsondatamodel;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;

public class ObjectMapper {

	private HashMap<Class<? extends Object>, Constructor<? extends JsonNode>> typeDict = new HashMap<Class<? extends Object>, Constructor<? extends JsonNode>>();

	public ObjectMapper() {
		try {
			typeDict.put(Integer.class, IntNode.class.getConstructor(Integer.TYPE));
			typeDict.put(Long.class, LongNode.class.getConstructor(Long.TYPE));
			typeDict.put(BigInteger.class, BigIntegerNode.class.getConstructor(BigInteger.class));
			typeDict.put(BigDecimal.class, DecimalNode.class.getConstructor(BigDecimal.class));
			typeDict.put(Double.class, DoubleNode.class.getConstructor(Double.TYPE));
			typeDict.put(Float.class, DoubleNode.class.getConstructor(Float.TYPE));
			typeDict.put(String.class, TextNode.class.getConstructor(String.class));
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		}

	}

	public JsonNode valueToTree(Object value) {

		if (value == null) {
			return NullNode.getInstance();
		}
		Class<? extends Object> valueClass = value.getClass();
		if(value instanceof JsonNode){
			return (JsonNode)value;
		}
		if(value instanceof StringBuilder){
			return TextNode.valueOf(((StringBuilder)value).toString());
		}
		if (valueClass.isArray()) {
			Object[] array = (Object[]) value;
			ArrayNode arrayNode = new ArrayNode();
			for (int i = 0; i < array.length; i++) {
				arrayNode.add(valueToTree(array[i]));
			}
			return arrayNode;
		}
		try {
			return typeDict.get(valueClass).newInstance(value);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		return NullNode.getInstance();

	}

}
