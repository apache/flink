package eu.stratosphere.sopremo.pact;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.expressions.ContainerExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;

public class SopremoUtil {
	public static final Log LOG = LogFactory.getLog(SopremoUtil.class);

	public static int getInputIndex(ContainerExpression expr) {
		InputSelection fragment = expr.find(InputSelection.class);
		if (fragment == null)
			return 0;
		return fragment.getIndex();
	}

	public static int getInputIndex(EvaluationExpression expr) {
		if (expr instanceof ContainerExpression)
			return getInputIndex((ContainerExpression) expr);
		else if (expr instanceof InputSelection)
			return ((InputSelection) expr).getIndex();
		return 0;
	}

	@SuppressWarnings("unchecked")
	public static <T> T deserialize(Configuration config, String key, Class<T> objectClass) {
		String string = config.getString(key, null);
		if (string == null)
			return null;
		return (T) stringToObject(string);
	}

	public static String objectToString(Object transformation) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try {
			ObjectOutputStream out = new ObjectOutputStream(bos);
			out.writeObject(transformation);
			out.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		String string = new String(Base64.encodeBase64(bos.toByteArray()));
		return string;
	}

	public static <T> T deserializeObject(ObjectInputStream ois, Class<T> clazz) throws IOException,
			ClassNotFoundException {
		if (ois.readBoolean())
			return (T) ois.readObject();

		T object;
		try {
			object = (T) Class.forName(ois.readUTF()).newInstance();
		} catch (InstantiationException e) {
			throw new IOException(e);
		} catch (IllegalAccessException e) {
			throw new IOException(e);
		}

		Map<String, Object> values = (Map<String, Object>) ois.readObject();
		BeanInfo beanInfo;
		try {
			beanInfo = Introspector.getBeanInfo(object.getClass());
		} catch (IntrospectionException e) {
			LOG.info(String.format("Cannot retrieve bean info for type %s: %s",
				object.getClass(), e.getMessage()));
			ois.readObject();
			return object;
		}

		for (PropertyDescriptor propertyDescriptor : beanInfo.getPropertyDescriptors()) {
			String name = propertyDescriptor.getName();
			if (values.containsKey(name))
				try {
					propertyDescriptor.getWriteMethod().invoke(object, values.get(name));
				} catch (Exception e) {
					LOG.debug(String.format("Cannot deserialize field %s of type %s: %s", propertyDescriptor.getName(),
						object.getClass(), e.getMessage()));
				}
		}

		return object;
	}

	public static void serializeObject(ObjectOutputStream oos, Object object) throws IOException {
		if (object instanceof Serializable) {
			oos.writeBoolean(true);
			oos.writeObject(object);
			return;
		}

		oos.writeBoolean(false);
		oos.writeUTF(object.getClass().getName());
		Map<String, Object> values = new HashMap<String, Object>();
		BeanInfo beanInfo;
		try {
			beanInfo = Introspector.getBeanInfo(object.getClass());
		} catch (IntrospectionException e) {
			LOG.info(String.format("Cannot retrieve bean info for type %s: %s",
				object.getClass(), e.getMessage()));
			oos.writeObject(values);
			return;
		}

		for (PropertyDescriptor propertyDescriptor : beanInfo.getPropertyDescriptors())
			if (Serializable.class.isAssignableFrom(propertyDescriptor.getPropertyType()) &&
					propertyDescriptor.getReadMethod() != null && propertyDescriptor.getWriteMethod() != null) {
				try {
					values.put(propertyDescriptor.getName(), propertyDescriptor.getReadMethod().invoke(object));
				} catch (Exception e) {
					LOG.debug(String.format("Cannot serialize field %s of type %s: %s", propertyDescriptor.getName(),
						object.getClass(), e.getMessage()));
				}
			}
		oos.writeObject(values);
	}

	public static void serialize(Configuration config, String key, Object object) {
		config.setString(key, objectToString(object));
	}

	public static void serializeNode(final DataOutput out, JsonNode value) throws IOException {
		final StringWriter writer = new StringWriter();
		JsonGenerator generator = JsonUtil.FACTORY.createJsonGenerator(writer);
		generator.setCodec(JsonUtil.OBJECT_MAPPER);
		generator.writeTree(value);
		SerializationString.get().setValue(writer.toString());
		SerializationString.get().write(out);
	}

	private static final ThreadLocal<PactString> SerializationString = new ThreadLocal<PactString>() {
		protected PactString initialValue() {
			return new PactString();
		};
	};

	@SuppressWarnings("unchecked")
	public static <T extends JsonNode> T deserializeNode(final DataInput in, Class<T> expectedClass) throws IOException {
		return (T) deserializeNode(in);
	}

	public static JsonNode deserializeNode(final DataInput in) throws IOException {
		SerializationString.get().read(in);
		JsonParser parser = JsonUtil.FACTORY.createJsonParser(SerializationString.get().getValue());
		parser.setCodec(JsonUtil.OBJECT_MAPPER);
		return parser.readValueAsTree();
	}

	public static void setContext(Configuration config, EvaluationContext context) {
		serialize(config, "context", context);
	}

	public static Object stringToObject(String string) {
		Object object = null;
		try {
			ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(Base64.decodeBase64(string
				.getBytes())));
			object = in.readObject();
			in.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return object;
	}
}
