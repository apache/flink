package eu.stratosphere.util.reflect;

public class DynamicInstance<Type> {
	private DynamicClass<Type> dynamicClass;

	private Type instance;

	public DynamicClass<Type> getDynamicClass() {
		return this.dynamicClass;
	}

	public DynamicInstance(DynamicClass<Type> dynamicClass, Object... params) {
		this.dynamicClass = dynamicClass;
		this.instance = dynamicClass.getConstructor().invoke(params);
	}

	public Object invoke(String name, Object... params) {
		return this.dynamicClass.invoke(this.instance, name, params);
	}
}
