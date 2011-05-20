package eu.stratosphere.reflect;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.dag.Navigator;

public class TypeSpecificHandler<InputType, OutputBase, Handler extends TypeHandler<InputType, OutputBase>> {

	private static class HandlerInfo<InputType, OutputBase, Handler extends TypeHandler<InputType, OutputBase>> {
		private Method callback;

		private int appendIndex = -1;

		private boolean stopRecursion = false;

		private Handler handler;

		@SuppressWarnings("unchecked")
		public <Type extends InputType> HandlerInfo(TypeHandler<Type, ? extends OutputBase> handler) {
			this.handler = (Handler) handler;

			Class<?> handlerClass = handler.getClass();
			Method[] methods = handlerClass.getMethods();
			for (Method method : methods)
				if (method.getDeclaringClass() == handler.getClass()) {
					method.setAccessible(true);
					this.callback = method;
					break;
				}

			AppendChildren annotation = this.getAnnotation(handlerClass, this.callback, AppendChildren.class);
			if (annotation != null)
				this.appendIndex = annotation.fromIndex();
			this.stopRecursion = this.getAnnotation(handlerClass, this.callback, Leaf.class) != null;
		}

		private <A extends Annotation> A getAnnotation(Class<?> handlerClass, Method callback, Class<A> annotationClass) {
			A annotation = handlerClass.getAnnotation(annotationClass);
			if (annotation == null)
				annotation = callback.getAnnotation(annotationClass);
			return annotation;
		}

		public boolean isStopRecursion() {
			return this.stopRecursion;
		}

		public Handler getHandler() {
			return this.handler;
		}

		public boolean shouldAppendChildren() {
			return this.appendIndex != -1;
		}

		public int getAppendIndex() {
			return this.appendIndex;
		}

		public Method getCallback() {
			return this.callback;
		}
	}

	private Map<Class<? extends InputType>, HandlerInfo> handlerInfos = new HashMap<Class<? extends InputType>, HandlerInfo>();

	private List<TypeHandlerListener<InputType, OutputBase>> handlerListeners = new ArrayList<TypeHandlerListener<InputType, OutputBase>>();

	public void addListener(TypeHandlerListener<InputType, OutputBase> listener) {
		this.handlerListeners.add(listener);
	}

	public void removeListener(TypeHandlerListener<InputType, OutputBase> listener) {
		this.handlerListeners.remove(listener);
	}

	@SuppressWarnings("unchecked")
	public <Type extends InputType> TypeSpecificHandler<InputType, OutputBase, Handler> register(
			TypeHandler<Type, ? extends OutputBase> handler) {
		this.register(handler,
			new Class[] { ReflectUtil.getBindingOfSuperclass(handler.getClass(), TypeHandler.class).getParameters()[0]
				.getType() });
		return this;
	}

	@SuppressWarnings("unchecked")
	public TypeSpecificHandler<InputType, OutputBase, Handler> registerAll(
			TypeHandler<? extends InputType, ? extends OutputBase>... handlers) {
		for (TypeHandler<? extends InputType, ? extends OutputBase> handler : handlers)
			this.register(handler,
				new Class[] { ReflectUtil.getBindingOfSuperclass(handler.getClass(), TypeHandler.class)
					.getParameters()[0].getType() });
		return this;
	}

	public <Type extends InputType> void register(TypeHandler<Type, ? extends OutputBase> handler,
			Class<? extends Type>... types) {
		HandlerInfo<InputType, OutputBase, Handler> handlerInfo = new HandlerInfo<InputType, OutputBase, Handler>(
			handler);
		for (Class<? extends Type> type : types)
			this.handlerInfos.put(type, handlerInfo);
		if (this.outputBase == null) {
			BoundType bounds = ReflectUtil.getBindingOfSuperclass(handler.getClass(), TypeHandler.class);
			this.outputBase = bounds.getParameters()[1].getType();
		}
	}

	public <Type extends InputType> void unregister(Class<Type> type) {
		this.handlerInfos.remove(type);
	}

	public void unregister(Handler type) {
		Iterator<? extends Entry<?, ?>> iterator = this.handlerInfos.entrySet().iterator();
		for (; iterator.hasNext();)
			if (iterator.next().getValue() == type)
				iterator.remove();
	}

	public OutputBase handle(InputType in, Object... params) {
		HandlerInfo<InputType, OutputBase, TypeHandler<InputType, OutputBase>> handlerInfo = this.getHandlerInfo(in
			.getClass());
		if (handlerInfo == null)
			return null;
		Object[] parameters = new Object[params.length + 1];
		parameters[0] = in;
		System.arraycopy(params, 0, parameters, 1, params.length);
		try {
			for (TypeHandlerListener<InputType, OutputBase> listener : this.handlerListeners)
				listener.beforeConversion(in, params);
			@SuppressWarnings("unchecked")
			OutputBase converted = (OutputBase) handlerInfo.callback.invoke(handlerInfo.handler, parameters);
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

	private List<OutputBase> lastChildren = Collections.EMPTY_LIST;

	public OutputBase handleRecursively(Navigator<InputType> navigator, InputType in, Object... params) {
		HandlerInfo<InputType, OutputBase, TypeHandler<InputType, OutputBase>> handlerInfo = this.getHandlerInfo(in
			.getClass());
		for (TypeHandlerListener<InputType, OutputBase> listener : this.handlerListeners)
			listener.beforeHierarchicalConversion(in, params);
		List<OutputBase> childTypes = new ArrayList<OutputBase>();

		if (handlerInfo == null || !handlerInfo.stopRecursion)
			for (InputType child : navigator.getConnectedNodes(in)) {
				OutputBase handledResult = this.handleRecursively(navigator, child, params);
				if (this.flattenCollection && handledResult instanceof Collection<?>)
					childTypes.addAll((Collection<? extends OutputBase>) handledResult);
				else if (handledResult != null)
					childTypes.add(handledResult);
			}
		childTypes.addAll(this.lastChildren);

		Object[] parameters = new Object[params.length + 1];
		parameters[0] = childTypes;
		this.lastChildren = handlerInfo != null && handlerInfo.shouldAppendChildren() ? childTypes.subList(
			handlerInfo.getAppendIndex(), childTypes.size()) : Collections.EMPTY_LIST;
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

	@SuppressWarnings("unchecked")
	public Handler getHandler(Class<?> clazz) {
		HandlerInfo<InputType, OutputBase, TypeHandler<InputType, OutputBase>> handlerInfo = this.getHandlerInfo(clazz);
		return handlerInfo == null ? null : (Handler) handlerInfo.handler;
	}

	private HandlerInfo<InputType, OutputBase, TypeHandler<InputType, OutputBase>> getHandlerInfo(Class<?> clazz) {
		HandlerInfo<InputType, OutputBase, TypeHandler<InputType, OutputBase>> handlerInfo = this.handlerInfos
			.get(clazz);
		if (handlerInfo == null && clazz.getSuperclass() != null) {
			handlerInfo = this.getHandlerInfo(clazz.getSuperclass());
			if (handlerInfo != null)
				this.handlerInfos.put((Class<? extends InputType>) clazz, handlerInfo);
		}
		return handlerInfo;
	}
}
