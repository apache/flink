package eu.stratosphere.util.reflect;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class DynamicMethod<ReturnType> extends DynamicInvokable<Method, Object, ReturnType> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2711357979080840754L;

	private Class<?> returnType;

	public DynamicMethod(String name) {
		super(name);
	}

	@Override
	public void addSignature(Method member) {
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
	protected ReturnType invokeDirectly(Method method, Object context, Object[] params) throws IllegalAccessException,
			InvocationTargetException {
		return (ReturnType) method.invoke(context, params);
	}

	@Override
	protected Method findMember(java.lang.Class<Object> clazz, java.lang.Class<?>[] parameterTypes)
			throws NoSuchMethodException {
		return clazz.getDeclaredMethod(this.getName(), parameterTypes);
	}

	public static DynamicMethod<?> valueOf(Class<?> clazz, String name) {
		DynamicMethod<?> method = new DynamicMethod<Object>(name);
		for (Method m : clazz.getDeclaredMethods())
			if (m.getName().equals(name))
				method.addSignature(m);
		return method;
	}
}
