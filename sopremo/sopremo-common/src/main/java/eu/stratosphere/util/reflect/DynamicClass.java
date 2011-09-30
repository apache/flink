package eu.stratosphere.util.reflect;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class DynamicClass<DeclaringClass> {
	private DynamicConstructor<DeclaringClass> constructor;

	private Map<String, DynamicInvokable<?, ?, ?>> methods = new HashMap<String, DynamicInvokable<?, ?, ?>>();

	private Class<DeclaringClass> declaringClass;

	private Map<String, Field> fields = new HashMap<String, Field>();

	public DynamicClass(Class<DeclaringClass> declaringClass) {
		this.declaringClass = declaringClass;
	}

	public synchronized DynamicConstructor<DeclaringClass> getConstructor() {
		if (this.constructor == null)
			this.constructor = DynamicConstructor.valueOf(this.declaringClass);
		return this.constructor;
	}

	private synchronized Field getField(String name) {
		Field field = this.fields.get(name);
		if (field == null)
			try {
				this.fields.put(name, field = this.declaringClass.getField(name));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		return field;
	}

	public Object getFieldValue(DeclaringClass instance, String name) {
		try {
			return this.getField(name).get(instance);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public synchronized DynamicInvokable<?, ?, ?> getMethod(String name) {
		DynamicInvokable<?, ?, ?> method = this.methods.get(name);
		if (method == null)
			this.methods.put(name, method = DynamicMethod.valueOf(this.declaringClass, name));
		return method;
	}

	public Object getStaticFieldValue(String name) {
		try {
			return this.getField(name).get(null);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public Object invoke(DeclaringClass instance, String name, Object... params) {
		return this.getMethod(name).invoke(instance, params);
	}

	public Object invokeStatic(String name, Object... params) {
		return this.getMethod(name).invoke(null, params);
	}

	public boolean isInstantiable() {
		return this.getConstructor().isInvokableFor();
	}

	public DynamicInstance<DeclaringClass> newDynamicInstance(Object... params) {
		return new DynamicInstance<DeclaringClass>(this, params);
	}

	public DeclaringClass newInstance(Object... params) {
		return this.getConstructor().invoke(params);
	}

	public void setFieldValue(DeclaringClass instance, String name, Object value) {
		try {
			this.getField(name).set(instance, value);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void setStaticFieldValue(String name, Object value) {
		try {
			this.getField(name).set(null, value);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
