package eu.stratosphere.util.reflect;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.util.FilteringIterable;
import eu.stratosphere.util.Predicate;

public class DynamicClass<DeclaringClass> {
	private DynamicConstructor<DeclaringClass> constructor;

	private final Map<String, DynamicInvokable<?, ?, ?>> methods = new HashMap<String, DynamicInvokable<?, ?, ?>>();

	private final Class<DeclaringClass> declaringClass;

	private final Map<String, Field> fields = new HashMap<String, Field>();

	private Map<String, DynamicProperty<?>> properties;

	private int stateMask;

	private final static int PROPERTY_INIT = 0x1;

	public DynamicClass(final Class<DeclaringClass> declaringClass) {
		this.declaringClass = declaringClass;
	}

	public synchronized DynamicConstructor<DeclaringClass> getConstructor() {
		if (this.constructor == null)
			this.constructor = DynamicConstructor.valueOf(this.declaringClass);
		return this.constructor;
	}

	private synchronized Field getField(final String name) {
		Field field = this.fields.get(name);
		if (field == null)
			try {
				this.fields.put(name, field = this.declaringClass.getField(name));
			} catch (final Exception e) {
				throw new RuntimeException(e);
			}
		return field;
	}

	public Object getFieldValue(final DeclaringClass instance, final String name) {
		try {
			return this.getField(name).get(instance);
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	public synchronized DynamicInvokable<?, ?, ?> getMethod(final String name) {
		DynamicInvokable<?, ?, ?> method = this.methods.get(name);
		if (method == null)
			this.methods.put(name, method = DynamicMethod.valueOf(this.declaringClass, name));
		return method;
	}

	public Object getStaticFieldValue(final String name) {
		try {
			return this.getField(name).get(null);
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	public Object invoke(final DeclaringClass instance, final String name, final Object... params) throws Exception {
		return this.getMethod(name).invoke(instance, params);
	}

	public Object invokeStatic(final String name, final Object... params) throws Exception {
		return this.getMethod(name).invoke(null, params);
	}

	public boolean isInstantiable() {
		return this.getConstructor().isInvokableFor();
	}

	public DynamicInstance<DeclaringClass> newDynamicInstance(final Object... params) throws Exception {
		return new DynamicInstance<DeclaringClass>(this, params);
	}

	public DeclaringClass newInstance(final Object... params) throws Exception {
		return this.getConstructor().invoke(params);
	}

	public void setFieldValue(final DeclaringClass instance, final String name, final Object value) {
		try {
			this.getField(name).set(instance, value);
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void setStaticFieldValue(final String name, final Object value) {
		try {
			this.getField(name).set(null, value);
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public <BaseType> Iterable<DynamicProperty<BaseType>> getProperties(final Class<BaseType> baseType) {
		return new FilteringIterable(this.getProperties(), new Predicate<DynamicProperty>() {
			@Override
			public boolean isTrue(final DynamicProperty property) {
				return baseType.isAssignableFrom(property.getType());
			}
		});
	}

	public boolean needsInit(final int stateBit) {
		return (this.stateMask & stateBit) == 0;
	}

	public void setState(final int stateBit) {
		this.stateMask |= stateBit;
	}

	public Iterable<DynamicProperty<?>> getProperties() {
		if (this.needsInit(PROPERTY_INIT))
			this.initProperties();
		return this.properties.values();
	}

	@SuppressWarnings("rawtypes")
	private void initProperties() {
		try {
			this.properties = new HashMap<String, DynamicProperty<?>>();
			for (final PropertyDescriptor property : Introspector.getBeanInfo(this.declaringClass)
				.getPropertyDescriptors())
				if (property.getPropertyType() != null)
					this.properties.put(property.getName(), new BeanProperty(property));
			this.setState(PROPERTY_INIT);
		} catch (final IntrospectionException e) {
			throw new RuntimeException(e);
		}
	}
}
