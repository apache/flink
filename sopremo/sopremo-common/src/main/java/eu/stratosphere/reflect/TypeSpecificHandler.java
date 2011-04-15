package eu.stratosphere.reflect;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.dag.Navigator;

public class TypeSpecificHandler<InputType, OutputBase, Handler extends TypeHandler<InputType, OutputBase>> {

	private Map<Class<? extends InputType>, Handler> handlers = new HashMap<Class<? extends InputType>, Handler>();

	private Map<Class<?>, Method> callbacks = new HashMap<Class<?>, Method>();

	private List<TypeHandlerListener<InputType, OutputBase>> handlerListeners = new ArrayList<TypeHandlerListener<InputType, OutputBase>>();

	public void addListener(TypeHandlerListener<InputType, OutputBase> listener) {
		this.handlerListeners.add(listener);
	}

	public void removeListener(TypeHandlerListener<InputType, OutputBase> listener) {
		this.handlerListeners.remove(listener);
	}

	public <Type extends InputType> TypeSpecificHandler<InputType, OutputBase, Handler> register(
			TypeHandler<Type, ? extends OutputBase> handler) {
		register(handler,
			new Class[] { ReflectUtil.getBindingOfSuperclass(handler.getClass(), TypeHandler.class).getParameters()[0]
				.getType() });
		return this;
	}

	public <Type extends InputType> void register(TypeHandler<Type, ? extends OutputBase> handler,
			Class<? extends Type>... types) {
		for (Class<? extends Type> type : types)
			this.handlers.put(type, (Handler) handler);
		Method[] methods = handler.getClass().getMethods();
		for (Method method : methods)
			if (method.getDeclaringClass() == handler.getClass()) {
				method.setAccessible(true);
				this.callbacks.put(handler.getClass(), method);
			}

		if (this.outputBase == null) {
			BoundType bounds = ReflectUtil.getBindingOfSuperclass(handler.getClass(), TypeHandler.class);
			this.outputBase = bounds.getParameters()[1].getType();
		}
	}

	public <Type extends InputType> void unregister(Class<Type> type) {
		this.handlers.remove(type);
	}

	public void unregister(Handler type) {
		Iterator<Entry<Class<? extends InputType>, Handler>> iterator = this.handlers.entrySet().iterator();
		for (; iterator.hasNext();)
			if (iterator.next().getValue() == type)
				iterator.remove();
	}

	public OutputBase handle(InputType in, Object... params) {
		Handler handler = this.getHandler(in.getClass());
		if (handler == null)
			return null;
		Object[] parameters = new Object[params.length + 1];
		parameters[0] = in;
		System.arraycopy(params, 0, parameters, 1, params.length);
		try {
			for (TypeHandlerListener<InputType, OutputBase> listener : this.handlerListeners)
				listener.beforeConversion(in, params);
			OutputBase converted = (OutputBase) this.callbacks.get(handler.getClass()).invoke(handler, parameters);
			for (TypeHandlerListener<InputType, OutputBase> listener : this.handlerListeners)
				listener.afterConversion(in, params, converted);
			return converted;
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
		// return handler.handle(in, params);
	}

	private boolean flattenCollection = true;

	private boolean passthroughChildren = true;

	private Class<?> outputBase;

	public OutputBase handleRecursively(Navigator<InputType> navigator, InputType in, Object... params) {
		for (TypeHandlerListener<InputType, OutputBase> listener : this.handlerListeners)
			listener.beforeHierarchicalConversion(in, params);
		List<OutputBase> childTypes = new ArrayList<OutputBase>();

		for (InputType child : navigator.getConnectedNodes(in)) {
			OutputBase handledResult = this.handleRecursively(navigator, child, params);
			if (this.flattenCollection && handledResult instanceof Collection<?>)
				childTypes.addAll((Collection<? extends OutputBase>) handledResult);
			else if (handledResult != null)
				childTypes.add(handledResult);
		}

		Object[] parameters = new Object[params.length + 1];
		parameters[0] = childTypes;
		System.arraycopy(params, 0, parameters, 1, params.length);
		OutputBase convertedType = this.handle(in, parameters);
		for (TypeHandlerListener<InputType, OutputBase> listener : this.handlerListeners)
			listener.afterHierarchicalConversion(in, params, convertedType);
		if (convertedType == null && this.passthroughChildren) {
			if (Collection.class.isAssignableFrom(this.outputBase))
				return (OutputBase) childTypes;
			return childTypes.isEmpty() ? null : childTypes.get(0);
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
