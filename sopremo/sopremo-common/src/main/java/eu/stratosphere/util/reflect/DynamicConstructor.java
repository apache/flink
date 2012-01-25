package eu.stratosphere.util.reflect;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class DynamicConstructor<DeclaringClass> extends
		DynamicInvokable<Constructor<DeclaringClass>, DeclaringClass, DeclaringClass> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3686535870776881782L;

	private Class<?> declaringClass;

	public DynamicConstructor() {
		super("<init>");
	}

	@Override
	public void addSignature(Constructor<DeclaringClass> member) {
		super.addSignature(member);
		if (this.declaringClass == null)
			this.declaringClass = member.getDeclaringClass();
		else if (member.getDeclaringClass() != this.declaringClass)
			this.declaringClass = member.getDeclaringClass().isAssignableFrom(this.declaringClass) ? this.declaringClass
				: member.getDeclaringClass();
	}

	@Override
	protected boolean isVarargs(Constructor<DeclaringClass> member) {
		return member.isVarArgs();
	}

	@Override
	protected Class<?>[] getParameterTypes(Constructor<DeclaringClass> member) {
		return member.getParameterTypes();
	}

	@Override
	protected DeclaringClass invokeDirectly(Constructor<DeclaringClass> member, Object context, Object[] params)
			throws IllegalAccessException, InvocationTargetException, IllegalArgumentException, InstantiationException {
		return member.newInstance(params);
	}

	public DeclaringClass invoke(Object... params) {
		return super.invoke(null, params);
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.util.reflect.DynamicInvokable#needsInstance(java.lang.reflect.Member)
	 */
	@Override
	protected boolean needsInstance(Constructor<DeclaringClass> member) {
		return false;
	}

	@Override
	protected Constructor<DeclaringClass> findMember(Class<DeclaringClass> clazz, Class<?>[] parameterTypes)
			throws NoSuchMethodException {
		return clazz.getDeclaredConstructor(parameterTypes);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Class<DeclaringClass> getReturnType() {
		return (Class<DeclaringClass>) this.declaringClass;
	}

	@SuppressWarnings("unchecked")
	public static <C> DynamicConstructor<C> valueOf(Class<C> clazz) {
		DynamicConstructor<C> ctor = new DynamicConstructor<C>();
		for (Constructor<?> constructor : clazz.getDeclaredConstructors())
			ctor.addSignature((Constructor<C>) constructor);
		return ctor;
	}
}
