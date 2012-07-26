package eu.stratosphere.util.reflect;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class DynamicMethod<ReturnType> extends DynamicInvokable<Method, Object, ReturnType> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2711357979080840754L;

	private Class<?> returnType;

	public DynamicMethod(final String name) {
		super(name);
	}

	@Override
	public void addSignature(final Method member) {
		super.addSignature(member);
		if (this.returnType == null)
			this.returnType = member.getReturnType();
		else if (member.getReturnType() != this.returnType)
			this.returnType = member.getReturnType().isAssignableFrom(this.returnType) ? this.returnType
				: member.getReturnType();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Class<ReturnType> getReturnType() {
		return (Class<ReturnType>) this.returnType;
	}

	@Override
	protected Class<?>[] getParameterTypes(final Method method) {
		return method.getParameterTypes();
	}

	@Override
	protected boolean isVarargs(final Method method) {
		return method.isVarArgs();
	}

	@SuppressWarnings("unchecked")
	@Override
	protected ReturnType invokeDirectly(final Method method, final Object context, final Object[] params)
			throws IllegalAccessException,
			InvocationTargetException {
		return (ReturnType) method.invoke(context, params);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.util.reflect.DynamicInvokable#needsInstance(java.lang.reflect.Member)
	 */
	@Override
	protected boolean needsInstance(final Method member) {
		return (member.getModifiers() & Modifier.STATIC) == 0;
	}

	@Override
	protected Method findMember(final java.lang.Class<Object> clazz, final java.lang.Class<?>[] parameterTypes)
			throws NoSuchMethodException {
		return clazz.getDeclaredMethod(this.getName(), parameterTypes);
	}

	public static DynamicMethod<?> valueOf(final Class<?> clazz, final String name) {
		final DynamicMethod<?> method = new DynamicMethod<Object>(name);
		for (final Method m : clazz.getDeclaredMethods())
			if (m.getName().equals(name))
				method.addSignature(m);
		return method;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.returnType.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		DynamicMethod<?> other = (DynamicMethod<?>) obj;
		return this.returnType.equals(other.returnType);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return getName();
	}
}
