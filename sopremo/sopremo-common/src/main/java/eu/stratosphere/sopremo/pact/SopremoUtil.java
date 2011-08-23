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
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.log4j.Level;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stub.Stub;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.expressions.ContainerExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;

public class SopremoUtil {
	public static final Log LOG = LogFactory.getLog(SopremoUtil.class);

	private static final ThreadLocal<PactString> SerializationString = new ThreadLocal<PactString>() {
		@Override
		protected PactString initialValue() {
			return new PactString();
		};
	};

	static void configureStub(final Stub<?, ?> stub, final Configuration parameters) {
		for (final Field stubField : stub.getClass().getDeclaredFields())
			if ((stubField.getModifiers() & (Modifier.TRANSIENT | Modifier.FINAL | Modifier.STATIC)) == 0)
				if (parameters.getString(stubField.getName(), null) != null)
					try {
						stubField.setAccessible(true);
						stubField.set(stub,
							SopremoUtil.deserialize(parameters, stubField.getName(), Serializable.class));
					} catch (final Exception e) {
						LOG.error(String.format("Could not set field %s of class %s: %s", stubField.getName(),
							stub.getClass(), e));
					}
	}

	@SuppressWarnings("unchecked")
	public static <T extends Serializable> T deserialize(final Configuration config, final String key,
			final Class<T> objectClass) {
		final String string = config.getString(key, null);
		if (string == null)
			return null;
		return (T) stringToObject(string);
	}

	public static JsonNode deserializeNode(final DataInput in) throws IOException {
		SerializationString.get().read(in);
		final JsonParser parser = JsonUtil.FACTORY.createJsonParser(SerializationString.get().getValue());
		parser.setCodec(JsonUtil.OBJECT_MAPPER);
		return parser.readValueAsTree();
	}

	@SuppressWarnings("unchecked")
	public static <T extends JsonNode> T deserializeNode(final DataInput in, final Class<T> expectedClass)
			throws IOException {
		return (T) deserializeNode(in);
	}

	@SuppressWarnings("unchecked")
	public static <T> T deserializeObject(final ObjectInputStream ois, final Class<T> clazz) throws IOException,
			ClassNotFoundException {
		if (ois.readBoolean())
			return (T) ois.readObject();

		T object;
		try {
			object = (T) Class.forName(ois.readUTF()).newInstance();
		} catch (final InstantiationException e) {
			throw new IOException(e);
		} catch (final IllegalAccessException e) {
			throw new IOException(e);
		}

		final Map<String, Object> values = (Map<String, Object>) ois.readObject();
		BeanInfo beanInfo;
		try {
			beanInfo = Introspector.getBeanInfo(object.getClass());
		} catch (final IntrospectionException e) {
			LOG.info(String.format("Cannot retrieve bean info for type %s: %s",
				object.getClass(), e.getMessage()));
			ois.readObject();
			return object;
		}

		for (final PropertyDescriptor propertyDescriptor : beanInfo.getPropertyDescriptors()) {
			final String name = propertyDescriptor.getName();
			if (values.containsKey(name))
				try {
					propertyDescriptor.getWriteMethod().invoke(object, values.get(name));
				} catch (final Exception e) {
					LOG.debug(String.format("Cannot deserialize field %s of type %s: %s", propertyDescriptor.getName(),
						object.getClass(), e.getMessage()));
				}
		}

		return object;
	}

	public static int getInputIndex(final ContainerExpression expr) {
		final InputSelection fragment = expr.find(InputSelection.class);
		if (fragment == null)
			return 0;
		return fragment.getIndex();
	}

	public static int getInputIndex(final EvaluationExpression expr) {
		if (expr instanceof ContainerExpression)
			return getInputIndex((ContainerExpression) expr);
		else if (expr instanceof InputSelection)
			return ((InputSelection) expr).getIndex();
		return 0;
	}

	public static String objectToString(final Object transformation) {
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try {
			final ObjectOutputStream out = new ObjectOutputStream(bos);
			out.writeObject(transformation);
			out.close();
		} catch (final IOException ex) {
			ex.printStackTrace();
		}
		final String string = new String(Base64.encodeBase64(bos.toByteArray()));
		return string;
	}

	public static void serialize(final Configuration config, final String key, final Serializable object) {
		config.setString(key, objectToString(object));
	}

	public static void serializeNode(final DataOutput out, final JsonNode value) throws IOException {
		final StringWriter writer = new StringWriter();
		final JsonGenerator generator = JsonUtil.FACTORY.createJsonGenerator(writer);
		generator.setCodec(JsonUtil.OBJECT_MAPPER);
		generator.writeTree(value);
		SerializationString.get().setValue(writer.toString());
		SerializationString.get().write(out);
	}

	public static void serializeObject(final ObjectOutputStream oos, final Object object) throws IOException {
		if (object instanceof Serializable) {
			oos.writeBoolean(true);
			oos.writeObject(object);
			return;
		}

		oos.writeBoolean(false);
		oos.writeUTF(object.getClass().getName());
		final Map<String, Object> values = new HashMap<String, Object>();
		BeanInfo beanInfo;
		try {
			beanInfo = Introspector.getBeanInfo(object.getClass());
		} catch (final IntrospectionException e) {
			LOG.info(String.format("Cannot retrieve bean info for type %s: %s",
				object.getClass(), e.getMessage()));
			oos.writeObject(values);
			return;
		}

		for (final PropertyDescriptor propertyDescriptor : beanInfo.getPropertyDescriptors())
			if (Serializable.class.isAssignableFrom(propertyDescriptor.getPropertyType()) &&
				propertyDescriptor.getReadMethod() != null && propertyDescriptor.getWriteMethod() != null)
				try {
					values.put(propertyDescriptor.getName(), propertyDescriptor.getReadMethod().invoke(object));
				} catch (final Exception e) {
					LOG.debug(String.format("Cannot serialize field %s of type %s: %s", propertyDescriptor.getName(),
						object.getClass(), e.getMessage()));
				}
		oos.writeObject(values);
	}

	public static void setContext(final Configuration config, final EvaluationContext context) {
		serialize(config, "context", context);
	}

	public static Object stringToObject(final String string) {
		Object object = null;
		try {
			final ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(Base64.decodeBase64(string
				.getBytes())));
			object = in.readObject();
			in.close();
		} catch (final IOException ex) {
			ex.printStackTrace();
		} catch (final ClassNotFoundException e) {
			e.printStackTrace();
		}
		return object;
	}

	public static void trace() {
		(((Log4JLogger) LOG).getLogger()).setLevel(Level.TRACE);
	}

	public static void untrace() {
		(((Log4JLogger) LOG).getLogger()).setLevel((((Log4JLogger) LOG).getLogger()).getParent().getLevel());
	}
}
