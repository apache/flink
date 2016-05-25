/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.graph;

import javassist.util.proxy.MethodFilter;
import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;
import javassist.util.proxy.ProxyObject;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.flink.api.java.DataSet;
import org.objenesis.ObjenesisStd;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link GraphAlgorithm} transforms an input {@link Graph} into an output of
 * type {@code T}. A {@code DelegatingGraphAlgorithm} wraps the algorithm
 * result with a delegating proxy object. The delegated object can be replaced
 * when the same algorithm is run on the same input with a mergeable configuration.
 * This allows algorithms to be composed of implicitly reusable algorithms
 * without publicly sharing intermediate {@link DataSet}s.
 *
 * @param <K> ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 * @param <T> output type
 */
public abstract class DelegatingGraphAlgorithm<K, VV, EV, T>
implements GraphAlgorithm<K, VV, EV, T> {

	// each algorithm and input pair may map to multiple configurations
	private static Map<DelegatingGraphAlgorithm, List<DelegatingGraphAlgorithm>> cache =
		Collections.synchronizedMap(new HashMap<DelegatingGraphAlgorithm, List<DelegatingGraphAlgorithm>>());

	private Graph<K,VV,EV> input;

	private Delegate<T> delegate;

	/**
	 * Algorithms are identified by name rather than by class to allow subclassing.
	 *
	 * @return name of the algorithm, which may be shared by multiple classes
	 *		 implementing the same algorithm and generating the same output
	 */
	protected abstract String getAlgorithmName();

	/**
	 * An algorithm must first test whether the configurations can be merged
	 * before merging individual fields.
	 *
	 * @param other the algorithm with which to compare and merge
	 * @returns true iff configuration has been merged and output can be reused
	 */
	protected abstract boolean mergeConfiguration(DelegatingGraphAlgorithm other);

	/**
	 * The implementation of the algorithm, renamed from {@link GraphAlgorithm#run(Graph)}.
	 *
	 * @param input the input graph
	 * @return the algorithm's output
	 * @throws Exception
	 */
	protected abstract T runInternal(Graph<K, VV, EV> input) throws Exception;

	@Override
	public final int hashCode() {
		return new HashCodeBuilder(17, 37)
			.append(input)
			.append(getAlgorithmName())
			.toHashCode();
	}

	@Override
	public final boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}

		if (obj == this) {
			return true;
		}

		if (! DelegatingGraphAlgorithm.class.isAssignableFrom(obj.getClass())) {
			return false;
		}

		DelegatingGraphAlgorithm rhs = (DelegatingGraphAlgorithm) obj;

		return new EqualsBuilder()
			.append(input, rhs.input)
			.append(getAlgorithmName(), rhs.getAlgorithmName())
			.isEquals();
	}

	@Override
	@SuppressWarnings("unchecked")
	public final T run(Graph<K, VV, EV> input)
			throws Exception {
		this.input = input;

		if (cache.containsKey(this)) {
			for (DelegatingGraphAlgorithm<K, VV, EV, T> other : cache.get(this)) {
				if (mergeConfiguration(other)) {
					// configuration has been merged so generate new output
					T output = runInternal(input);

					// update delegatee object and reuse delegate
					other.delegate.setObject(output);
					delegate = other.delegate;

					return delegate.getProxy();
				}
			}
		}

		// no mergeable configuration found so generate new output
		T output = runInternal(input);

		// create a new delegate to wrap the algorithm output
		delegate = new Delegate<T>(output);

		// cache this result
		if (cache.containsKey(this)) {
			cache.get(this).add(this);
		} else {
			cache.put(this, new ArrayList(Collections.singletonList(this)));
		}

		return delegate.getProxy();
	}

	/**
	 * Wraps an object with a proxy delegate whose method handler invokes all
	 * method calls on the wrapped object. This object can be later replaced.
	 *
	 * @param <X>
	 */
	private static class Delegate<X> {
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
			this.obj = obj;
		}

		/**
		 * Instantiates and returns a proxy object which subclasses the
		 * delegated object. The proxy's method handler invokes all methods
		 * on the delegated object that is set at the time of invocation.
		 *
		 * @return delegating proxy
		 */
		public X getProxy() {
			if (proxy != null) {
				return proxy;
			}

			ProxyFactory factory = new ProxyFactory();
			factory.setSuperclass(obj.getClass());

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
					// method visibility may be restricted
					thisMethod.setAccessible(true);
					return thisMethod.invoke(obj, args);
				}
			});

			return proxy;
		}
	}
}
