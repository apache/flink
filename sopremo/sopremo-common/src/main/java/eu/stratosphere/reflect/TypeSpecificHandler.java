package eu.stratosphere.reflect;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.dag.Navigator;
import eu.stratosphere.pact.common.util.ReflectionUtil;

public class TypeSpecificHandler<InputType, OutputBase, Handler extends TypeHandler<InputType, OutputBase>> {

	private Map<Class<? extends InputType>, Handler> handlers = new HashMap<Class<? extends InputType>, Handler>();

	private Map<Class<?>, Method> callbacks = new HashMap<Class<?>, Method>();

	public <Type extends InputType> void register(Class<Type> type, TypeHandler<Type, ? extends OutputBase> handler) {
		handlers.put(type, (Handler) handler);
		Method[] methods = handler.getClass().getMethods();
		for (Method method : methods) {
			if (method.getDeclaringClass() == handler.getClass()) {
				method.setAccessible(true);
				callbacks.put(handler.getClass(), method);
			}
		}

		if (outputBase == null) {
			BoundType bounds = ReflectUtil.getBindingOfSuperclass(handler.getClass(), TypeHandler.class);
			outputBase = bounds.getParameters()[1].getType();
		}
	}

	public <Type extends InputType> void unregister(Class<Type> type) {
		handlers.remove(type);
	}

	public void unregister(Handler type) {
		Iterator<Entry<Class<? extends InputType>, Handler>> iterator = handlers.entrySet().iterator();
		for (; iterator.hasNext();) {
			if (iterator.next().getValue() == type)
				iterator.remove();
		}
	}

	public OutputBase handle(InputType in, Object... params) {
		Handler handler = getHandler(in.getClass());
		if (handler == null)
			return null;
		Object[] parameters = new Object[params.length + 1];
		parameters[0] = in;
		System.arraycopy(params, 0, parameters, 1, params.length);
		try {
			return (OutputBase) callbacks.get(handler.getClass()).invoke(handler, parameters);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
		// return handler.handle(in, params);
	}

	private boolean flattenCollection = true;

	private boolean passthroughChildren = true;

	private Class<?> outputBase;

	public OutputBase handleRecursively(Navigator<InputType> navigator, InputType in, Object... params) {
		List<OutputBase> convertedTypes = new ArrayList<OutputBase>();

		for (InputType child : navigator.getConnectedNodes(in)) {
			OutputBase handledResult = handleRecursively(navigator, child, params);
			if (flattenCollection && handledResult instanceof Collection<?>)
				convertedTypes.addAll((Collection<? extends OutputBase>) handledResult);
			else if (handledResult != null)
				convertedTypes.add(handledResult);
		}

		Object[] parameters = new Object[params.length + 1];
		parameters[0] = convertedTypes;
		System.arraycopy(params, 0, parameters, 1, params.length);
		OutputBase convertedType = handle(in, parameters);
		if (convertedType == null && passthroughChildren) {
			if (Collection.class.isAssignableFrom(outputBase))
				return (OutputBase) convertedTypes;
			return convertedTypes.isEmpty() ? null : convertedTypes.get(0);
		}
		return convertedType;
	}

	public Handler getHandler(Class<?> clazz) {
		Handler handler = this.handlers.get(clazz);
		if (handler == null && clazz.getSuperclass() != null) {
			handler = this.getHandler(clazz.getSuperclass());
			if (handler != null)
				this.handlers.put((Class<? extends InputType>) clazz, handler);
		}
		return handler;
	}
}
