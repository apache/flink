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
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.SimpleLog;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.util.StringUtils;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.sopremo.expressions.CachingExpression;
import eu.stratosphere.sopremo.expressions.ContainerExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.type.AbstractJsonNode.Type;
import eu.stratosphere.sopremo.type.BigIntegerNode;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.DecimalNode;
import eu.stratosphere.sopremo.type.DoubleNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IPrimitiveNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.LongNode;
import eu.stratosphere.util.reflect.BoundType;
import eu.stratosphere.util.reflect.ReflectUtil;

/**
 * Provides utility methods for sopremo
 */
public class SopremoUtil {

	public static final boolean DEBUG = true;

	public static final Log NORMAL_LOG = LogFactory.getLog(SopremoUtil.class), TRACE_LOG = new SimpleLog(
		SopremoUtil.class.getName());

	static {
		((SimpleLog) TRACE_LOG).setLevel(SimpleLog.LOG_LEVEL_TRACE);
	}

	public static Log LOG = NORMAL_LOG;

	public static final String CONTEXT = "context";

	/**
	 * Configures a {@link Stub} with the given {@link Configuration}
	 * 
	 * @param stub
	 *        the stub that should be configured
	 * @param parameters
	 *        the configuration that should be used
	 */
	static void configureStub(final Stub stub, final Configuration parameters) {
		final Class<? extends Stub> stubClass = stub.getClass();
		for (final Field stubField : stubClass.getDeclaredFields())
			if ((stubField.getModifiers() & (Modifier.TRANSIENT
				| Modifier.FINAL | Modifier.STATIC)) == 0)
				if (parameters.getString(stubField.getName(), null) != null)
					try {
						stubField.setAccessible(true);
						stubField.set(stub, SopremoUtil
							.deserializeCachingAware(parameters,
								stubField.getName(),
								stubField.getType(),
								stubField.getGenericType(),
								stubClass.getClassLoader()));
					} catch (final Exception e) {
						LOG.error(String.format(
							"Could not set field %s of class %s: %s",
							stubField.getName(), stubClass,
							StringUtils.stringifyException(e)));
					}
	}

	/**
	 * Deserializes the value that is stored for the given key in the configuration
	 * 
	 * @param config
	 *        the {@link Configuration} that holds the serialized values
	 * @param key
	 *        the key
	 * @param objectClass
	 *        the class of the value which is expected
	 * @return the deserialized value of the given key
	 */
	@SuppressWarnings("unchecked")
	public static Object deserializeCachingAware(final Configuration config, final String key,
			final Class<?> targetRawType, final java.lang.reflect.Type targetType, final ClassLoader classLoader) {

		final Object object = deserialize(config, key, Serializable.class, classLoader);
		if (CachingExpression.class.isAssignableFrom(targetRawType)
			&& !(object instanceof CachingExpression)) {
			final Class<IJsonNode> cachingType;
			if (targetType instanceof ParameterizedType)
				cachingType = (Class<IJsonNode>) BoundType.of(
					(ParameterizedType) targetType).getParameters()[0]
					.getType();
			else
				cachingType = IJsonNode.class;
			return CachingExpression.of((EvaluationExpression) object,
				cachingType);
		}
		return object;
	}

	public static <T extends Serializable> T deserialize(final Configuration config, final String key,
			final Class<T> objectClass) {

		return deserialize(config, key, objectClass, ClassLoader.getSystemClassLoader());

	}

	@SuppressWarnings("unchecked")
	public static <T extends Serializable> T deserialize(final Configuration config, final String key,
			@SuppressWarnings("unused") final Class<T> objectClass, final ClassLoader classLoader) {
		final String string = config.getString(key, null);
		if (string == null)
			return null;
		return (T) stringToObject(string, classLoader);
	}

	/**
	 * Deserializes a {@link IJsonNode} from a given {@link DataInput}
	 * 
	 * @param in
	 *        the datainput that contains the serialized node
	 * @return the deserialized node
	 */

	public static IJsonNode deserializeNode(final DataInput in) throws IOException {
		IJsonNode value = null;
		try {
			final int readInt = in.readInt();
			if (readInt == Type.CustomNode.ordinal()) {
				final String className = in.readUTF();
				value = (IJsonNode) ReflectUtil.newInstance(Class.forName(className));
			} else
				value = ReflectUtil.newInstance(Type.values()[readInt].getClazz());
			value.read(in);
		} catch (final ClassNotFoundException e) {
			throw new IllegalStateException("Cannot instantiate value because class is not in class path", e);
		}

		return value.canonicalize();
	}

	/**
	 * Deserializes an Object that was serialized with <code>SopremoUtil.serializeObject()</code> from a given
	 * {@link ObjectInputStream}
	 * 
	 * @param ois
	 *        the stream that contains the serialized object
	 * @param clazz
	 *        the class of the object that is expected
	 * @return the deserialized object
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	@SuppressWarnings("unchecked")
	public static <T> T deserializeObject(final ObjectInputStream ois, @SuppressWarnings("unused") final Class<T> clazz)
			throws IOException, ClassNotFoundException {
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

		final Map<String, Object> values = (Map<String, Object>) ois
			.readObject();
		BeanInfo beanInfo;
		try {
			beanInfo = Introspector.getBeanInfo(object.getClass());
		} catch (final IntrospectionException e) {
			LOG.info(String.format("Cannot retrieve bean info for type %s: %s",
				object.getClass(), e.getMessage()));
			ois.readObject();
			return object;
		}

		for (final PropertyDescriptor propertyDescriptor : beanInfo
			.getPropertyDescriptors()) {
			final String name = propertyDescriptor.getName();
			if (values.containsKey(name))
				try {
					propertyDescriptor.getWriteMethod().invoke(object,
						values.get(name));
				} catch (final Exception e) {
					LOG.debug(String.format(
						"Cannot deserialize field %s of type %s: %s",
						propertyDescriptor.getName(), object.getClass(),
						e.getMessage()));
				}
		}

		return object;
	}

	/**
	 * Returns the index from an {@link InputSelection} that is stored in a {@link ContainerExpression}. This index is
	 * not the position of the InputSelection within the ContainerNode, it is the index of the input that will be
	 * selected by the InputSelection.
	 * 
	 * @param expr
	 *        the ContainerExpression that should be used
	 * @return the index from the first InputSelection or 0 if no InputSelection exists
	 */
	public static int getInputIndex(final ContainerExpression expr) {
		final InputSelection fragment = expr.find(InputSelection.class);
		if (fragment == null)
			return 0;
		return fragment.getIndex();
	}

	/**
	 * Returns the index from an {@link InputSelection}. The given {@link EvaluationExpression} must be either an
	 * InputSelection itself or a {@link ContainerExpression} that have stored an InputSelection. In all other cases 0
	 * is returned.
	 * 
	 * @param expr
	 *        the EvaluationExpression that should be used
	 * @return the index from the InputSelection or 0 in the described cases
	 */
	public static int getInputIndex(final EvaluationExpression expr) {
		if (expr instanceof ContainerExpression)
			return getInputIndex((ContainerExpression) expr);
		else if (expr instanceof InputSelection)
			return ((InputSelection) expr).getIndex();
		return 0;
	}

	/**
	 * Converts an Object to a String. This String represents the Object and can be converted back with
	 * <code>SopremoUtil.stringToObject()</code>
	 * 
	 * @param transformation
	 *        the Object that should be converted
	 * @return the String representation
	 */
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

	/**
	 * Converts the given Object to a String and saves it for the key in the given {@link Configuration}
	 * 
	 * @param config
	 *        the Configuration that should be used
	 * @param key
	 *        the key
	 * @param object
	 *        the Object that should be saved
	 */
	public static void serialize(final Configuration config, final String key, final Serializable object) {
		config.setString(key, objectToString(object));
	}

	/**
	 * Serializes the given {@link IJsonNode} into the given {@link DataOutput}
	 * 
	 * @param out
	 *        the DataOutput that should be used for serialization
	 * @param iJsonNode
	 *        the IJsonNode that should be serialized
	 */
	public static void serializeNode(final DataOutput out, final IJsonNode iJsonNode) {

		try {
			out.writeInt(iJsonNode.getType().ordinal());

			if (iJsonNode.getType() == Type.CustomNode)
				out.writeUTF(iJsonNode.getClass().getName());
			iJsonNode.write(out);
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Serializes an Object into the given {@link ObjectOutputStream}. The serialized Object can than be deserialized
	 * with <code>SopremoUtil.deserializeObject()</code>
	 * 
	 * @param oos
	 *        the stream that should be used for serialization
	 * @param object
	 *        the object that should be serialized
	 * @throws IOException
	 */
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

		for (final PropertyDescriptor propertyDescriptor : beanInfo
			.getPropertyDescriptors())
			if (Serializable.class.isAssignableFrom(propertyDescriptor
				.getPropertyType())
				&& propertyDescriptor.getReadMethod() != null
				&& propertyDescriptor.getWriteMethod() != null)
				try {
					values.put(propertyDescriptor.getName(), propertyDescriptor
						.getReadMethod().invoke(object));
				} catch (final Exception e) {
					LOG.debug(String.format(
						"Cannot serialize field %s of type %s: %s",
						propertyDescriptor.getName(), object.getClass(),
						e.getMessage()));
				}
		oos.writeObject(values);
	}

	/**
	 * Converts a String representation of an object to Object. To be successful the given String should be a result of
	 * <code>SopremoUtil.objectToString</code>
	 * 
	 * @param string
	 *        the String that should be converted
	 * @return the object
	 */
	public static Object stringToObject(final String string) {
		return stringToObject(string, ClassLoader.getSystemClassLoader());
	}

	public static Object stringToObject(final String string,
			final ClassLoader classLoader) {
		Object object = null;
		try {
			final ObjectInputStream in = new CLObjectInputStream(
				new ByteArrayInputStream(Base64.decodeBase64(string
					.getBytes())), classLoader);
			object = in.readObject();
			in.close();
		} catch (final IOException ex) {
			ex.printStackTrace();
		} catch (final ClassNotFoundException e) {
			LOG.error(String.format("%s; classpath %s", e.getMessage(),
				System.getProperty("java.class.path")));
			e.printStackTrace();
		}
		return object;
	}

	public static void trace() {
		LOG = TRACE_LOG;
	}

	public static void untrace() {
		LOG = NORMAL_LOG;
	}

	/**
	 * Unwraps the {@link IJsonNode} that is wrapped by the given {@link JsonNodeWrapper}. Ensures that only
	 * JsonNodeWrapper are unwrapped.
	 * 
	 * @param wrapper
	 *        the JsonNodeWrapper that should be unwrapped
	 * @return the wrapped node
	 */
	public static IJsonNode unwrap(final IJsonNode wrapper) {
		if (!(wrapper instanceof JsonNodeWrapper))
			return wrapper;
		return ((JsonNodeWrapper) wrapper).getValue();
	}

	/**
	 * Wraps the given {@link IJsonNode} with a {@link JsonNodeWrapper}. Ensures that only non-JsonNodeWrapper are
	 * wrapped.
	 * 
	 * @param node
	 *        the IJsonNode that should be wrapped
	 * @return the wrapped node
	 */
	public static IJsonNode wrap(final IJsonNode node) {
		if (node instanceof JsonNodeWrapper)
			return node;
		return new JsonNodeWrapper(node);
	}

	/**
	 * Prepares the given {@link IJsonNode} for reusage. If the given node can be reused his content is cleared,
	 * otherwise a new instance will be created.
	 * 
	 * @param target
	 *        the IJsonNode that should be reused
	 * @param clazz
	 *        the class that is expected for reusage
	 * @return the IJsonNode that is prepared for reusage
	 */
	@SuppressWarnings("unchecked")
	public static <T extends IJsonNode> T reinitializeTarget(IJsonNode target, final Class<T> clazz) {
		if (target == null || !clazz.isInstance(target))
			target = ReflectUtil.newInstance(clazz).canonicalize();
		else
			target.clear();
		return (T) target;
	}

	@SuppressWarnings("unchecked")
	public static <T extends IJsonNode> T ensureType(IJsonNode target, final Class<T> clazz) {
		if (target == null || !clazz.isInstance(target))
			target = ReflectUtil.newInstance(clazz).canonicalize();
		return (T) target;
	}

	@SuppressWarnings("unchecked")
	public static <T extends IJsonNode> T ensureType(IJsonNode target, final Class<T> interfaceType,
			final Class<? extends T> defaultInstantation) {
		if (target == null || !interfaceType.isInstance(target))
			target = ReflectUtil.newInstance(defaultInstantation).canonicalize();
		return (T) target;
	}

	/**
	 * Reuses a {@link IJsonNode} by simply copying the value from the source to the target node. The value will not be
	 * copied if the source and target are no {@link IPrimitiveNode}s
	 * 
	 * @param source
	 *        the IJsonNode that should be used as the source
	 * @param target
	 *        the IJsonNode that should be used as the target
	 * @return the reused IJsonNode
	 */
	public static IJsonNode reusePrimitive(final IJsonNode source,
			final IJsonNode target) {
		final Class<? extends IJsonNode> sourceClass = source.getClass();
		if (sourceClass != target.getClass()
			|| sourceClass.equals(BooleanNode.class) || source.isNull()) {
			return source;
		}
		if (!(source instanceof IPrimitiveNode)) {
			return source;
		}

		if (sourceClass.equals(IntNode.class))
			((IntNode) target).setValue(((IntNode) source).getIntValue());
		else if (sourceClass.equals(DoubleNode.class))
			((DoubleNode) target).setValue(((DoubleNode) source)
				.getDoubleValue());
		else if (sourceClass.equals(LongNode.class))
			((LongNode) target).setValue(((LongNode) source).getLongValue());
		else if (sourceClass.equals(DecimalNode.class))
			((DecimalNode) target).setValue(((DecimalNode) source)
				.getDecimalValue());
		else if (sourceClass.equals(BigIntegerNode.class))
			((BigIntegerNode) target).setValue(((BigIntegerNode) source)
				.getBigIntegerValue());
		return target;
	}

	private static class CLObjectInputStream extends ObjectInputStream {

		private final ClassLoader classLoader;

		private CLObjectInputStream(final InputStream in,
				final ClassLoader classLoader) throws IOException {
			super(in);
			this.classLoader = classLoader;
		}

		/*
		 * (non-Javadoc)
		 * @see
		 * java.io.ObjectInputStream#resolveClass(java.io.ObjectStreamClass)
		 */
		@Override
		protected Class<?> resolveClass(final ObjectStreamClass desc)
				throws IOException, ClassNotFoundException {
			try {
				return super.resolveClass(desc);
			} catch (final ClassNotFoundException e) {
				return this.classLoader.loadClass(desc.getName());
			}
		}
	}
}
