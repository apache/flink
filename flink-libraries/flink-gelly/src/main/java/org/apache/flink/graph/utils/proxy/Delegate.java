/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.graph.utils.proxy;

import javassist.util.proxy.MethodFilter;
import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;
import javassist.util.proxy.ProxyObject;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.Operator;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.util.Preconditions;
import org.objenesis.ObjenesisStd;

import java.lang.reflect.Method;

/**
 * Wraps an object with a proxy delegate whose method handler invokes all
 * method calls on the wrapped object. This object can be later replaced.
 *
 * @param <X> the type of the proxied object
 */
public class Delegate<X> {
	private X obj;

	private X proxy = null;

	/**
	 * Set the initial delegated object.
	 *
	 * @param obj delegated object
	 */
	public Delegate(X obj) {
		setObject(obj);
	}

	/**
	 * Change the delegated object.
	 *
	 * @param obj delegated object
	 */
	public void setObject(X obj) {
		this.obj = (obj instanceof ReferentProxy) ? ((ReferentProxy<X>) obj).getProxiedObject() : obj;
	}

	/**
	 * Instantiates and returns a proxy object which subclasses the
	 * delegated object. The proxy's method handler invokes all methods
	 * on the delegated object that is set at the time of invocation.
	 *
	 * @return delegating proxy
	 */
	@SuppressWarnings("unchecked")
	public X getProxy() {
		if (proxy != null) {
			return proxy;
		}

		Class<?> superclass = obj.getClass();
		while (! superclass.getSuperclass().equals(Operator.class)) {
			superclass = superclass.getSuperclass();
		}

		ProxyFactory factory = new ProxyFactory();
		factory.setSuperclass(superclass);
		factory.setInterfaces(new Class[]{ReferentProxy.class});

		// create the class and instantiate an instance without calling a constructor
		Class<? extends X> proxyClass = factory.createClass(new MethodFilter() {
			@Override
			public boolean isHandled(Method method) {
				return true;
			}
		});
		proxy = new ObjenesisStd().newInstance(proxyClass);

		// create and set a handler to invoke all method calls on the delegated object
		((ProxyObject) proxy).setHandler(new MethodHandler() {
			@Override
			public Object invoke(Object self, Method thisMethod, Method proceed, Object[] args) throws Throwable {
				if (thisMethod.getName().equals("getProxiedObject")) {
					// this method is provided by the ReferentProxy interface
					return obj;
				} else {
					Method objMethod;
					try {
						// lookup public method from class hierarchy
						objMethod = obj.getClass().getMethod(thisMethod.getName(), thisMethod.getParameterTypes());
					} catch(NoSuchMethodException ignored) {
						// lookup private method defined on class
						objMethod = obj.getClass().getDeclaredMethod(thisMethod.getName(), thisMethod.getParameterTypes());
					}

					Class<?>[] parameterTypes = thisMethod.getParameterTypes();

					// reinterpret DataSet.output to add the delegate to the list of data sinks; otherwise, the proxy
					// object will be added to the list of sinks and cannot be replaced
					if (thisMethod.getName().equals("output") && parameterTypes.length == 1 && parameterTypes[0].equals(OutputFormat.class)) {
						OutputFormat<?> outputFormat = (OutputFormat<?>)args[0];
						Preconditions.checkNotNull(outputFormat);

						ExecutionEnvironment context = ((DataSet<?>)obj).getExecutionEnvironment();
						TypeInformation<?> type = ((DataSet<?>)obj).getType();

						// configure the type if needed
						if (outputFormat instanceof InputTypeConfigurable) {
							((InputTypeConfigurable) outputFormat).setInputType(type, context.getConfig() );
						}

						DataSink<?> sink = new DataSink((DataSet)self, outputFormat, type);

						Method outputMethod = ExecutionEnvironment.class.getDeclaredMethod("registerDataSink", DataSink.class);
						outputMethod.setAccessible(true);
						outputMethod.invoke(context, sink);
						return sink;
					} else {
						// method visibility may be restricted
						objMethod.setAccessible(true);
						return objMethod.invoke(obj, args);
					}
				}
			}
		});

		return proxy;
	}

	/**
	 * This interface provides access via the proxy handler to the original
	 * object being proxied. This is necessary since we cannot and should not
	 * create a proxy of a proxy but must instead proxy the original object.
	 *
	 * @param <Y> the type of the proxied object
	 */
	protected interface ReferentProxy<Y> {
		Y getProxiedObject();
	}
}
