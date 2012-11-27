package eu.stratosphere.util.reflect;

public class DynamicInstance<Type> {
	private final DynamicClass<Type> dynamicClass;

	private final Type instance;

	public DynamicClass<Type> getDynamicClass() {
		return this.dynamicClass;
	}

	public DynamicInstance(final DynamicClass<Type> dynamicClass, final Object... params) throws Exception {
		this.dynamicClass = dynamicClass;
		this.instance = dynamicClass.getConstructor().invoke(params);
	}

	public Object invoke(final String name, final Object... params) throws Exception {
		return this.dynamicClass.invoke(this.instance, name, params);
	}
}
