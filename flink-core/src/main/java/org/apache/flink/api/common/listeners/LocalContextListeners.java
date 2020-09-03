/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.listeners;

import org.apache.flink.api.common.ExecutionConfig;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages the lifecycle of {@link LocalContextListener} instances.
 */
public class LocalContextListeners {
	private static final Map<ClassLoader, ContextState> contextStates = new WeakHashMap<>();

	public static void open(ClassLoader classLoader, ExecutionConfig executionConfig)
		throws Exception {
		ContextState contextState;
		synchronized (contextStates) {
			contextState = contextStates.computeIfAbsent(classLoader, ContextState::new);
		}
		contextState.open(classLoader, executionConfig);
	}

	public static void close(ClassLoader classLoader, ExecutionConfig executionConfig)
		throws Exception {
		ContextState contextState;
		synchronized (contextStates) {
			contextState = contextStates.computeIfAbsent(classLoader, ContextState::new);
		}
		contextState.close(classLoader, executionConfig);
	}

	private static class ContextState {
		/**
		 * The listeners should be singleton-like instances for their classloader. There may be more than one
		 * "singleton" if the same class is in multiple classloaders, but for any given classloader the listener should
		 * be unique.
		 */
		private final Map<Class, LocalContextListener> listenerSingletons = new WeakHashMap<>();
		/**
		 * The count of calls to {@link #open(ClassLoader, ExecutionConfig)} without a matching call to {@link
		 * #close(ClassLoader, ExecutionConfig)}.
		 */
		private final AtomicInteger openCount = new AtomicInteger();
		/**
		 * The listeners for which {@link LocalContextListener#openContext(ExecutionConfig)} have been called and which
		 * will require a corresponding call to {@link LocalContextListener#closeContext(ExecutionConfig)}.
		 */
		private final List<LocalContextListener> contextOpened = new LinkedList<>();
		/**
		 * The listeners for which {@link LocalContextListener#openThread(ExecutionConfig)} have been called and which
		 * will require a corresponding call to {@link LocalContextListener#closeThread(ExecutionConfig)}.
		 */
		private final ThreadLocal<List<LocalContextListener>> threadOpened = ThreadLocal.withInitial(LinkedList::new);

		public ContextState(ClassLoader ignored) {
		}

		private static LocalContextListener createListener(Class<? extends LocalContextListener> clazz) {
			try {
				return clazz.newInstance();
			} catch (Exception e) {
				throw new RuntimeException("Cannot create local context listener " + clazz.getName());
			}
		}

		private List<LocalContextListener> listeners(ClassLoader classLoader, ExecutionConfig executionConfig) {
			ClassLoader oldTCCL = Thread.currentThread().getContextClassLoader();
			try {
				final LinkedHashSet<Class<? extends LocalContextListener>> listenerClasses =
					executionConfig.getRegisteredLocalContextListeners();
				List<LocalContextListener> result = new ArrayList<>(listenerClasses.size());
				for (Class<? extends LocalContextListener> listenerClass : listenerClasses) {
					synchronized (listenerSingletons) {
						Thread.currentThread().setContextClassLoader(classLoader);
						result.add(listenerSingletons.computeIfAbsent(listenerClass, ContextState::createListener));
					}
				}
				return result;
			} finally {
				Thread.currentThread().setContextClassLoader(oldTCCL);
			}
		}

		private synchronized void open(ClassLoader classLoader, ExecutionConfig executionConfig) throws Exception {
			final List<LocalContextListener> listeners = listeners(classLoader, executionConfig);
			// first check if we are the "first" call to open for this context
			// need not be the strict first, providing that there were matching calls to close
			// after each prior "first" open
			ClassLoader oldTCCL = Thread.currentThread().getContextClassLoader();
			try {
				// TODO ideally decide if we should verify the executionConfig remains consistent
				boolean openingContext = openCount.getAndIncrement() == 0;
				if (openingContext) {
					contextOpened.clear();
				}
				for (LocalContextListener listener : listeners) {
					if (openingContext || !contextOpened.contains(listener)) {
						try {
							Thread.currentThread().setContextClassLoader(classLoader);
							listener.openContext(executionConfig);
							contextOpened.add(0, listener);
						} catch (Exception e) {
							if (openingContext) {
								try {
									LocalContextListeners.close(classLoader, executionConfig);
								} catch (Exception e1) {
									e.addSuppressed(e1);
								}
							}
							throw e;
						}
					}
				}
				// now initialize the thread, again keeping track of what we have initialized in order to unwind
				// in the event of failures
				List<LocalContextListener> opened = threadOpened.get();
				for (LocalContextListener listener : listeners) {
					try {
						Thread.currentThread().setContextClassLoader(classLoader);
						listener.openThread(executionConfig);
						opened.add(0, listener);
					} catch (Exception e) {
						try {
							LocalContextListeners.close(classLoader, executionConfig);
						} catch (Exception e1) {
							e.addSuppressed(e1);
						}
						throw e;
					}
				}
			} finally {
				Thread.currentThread().setContextClassLoader(oldTCCL);
			}
		}

		private synchronized void close(ClassLoader classLoader, ExecutionConfig executionConfig) throws Exception {
			// Close should be the reverse order from open
			Exception exception = null;
			ClassLoader oldTCCL = Thread.currentThread().getContextClassLoader();
			try {
				final List<LocalContextListener> opened = threadOpened.get();
				threadOpened.remove();
				for (LocalContextListener listener : opened) {
					try {
						Thread.currentThread().setContextClassLoader(classLoader);
						listener.closeThread(executionConfig);
					} catch (Exception e) {
						if (exception == null) {
							exception = e;
						} else {
							exception.addSuppressed(e);
						}
					}
				}
				// check if this was the "first" initialization and unwind the context if that is the case
				if (openCount.decrementAndGet() == 0) {
					try {
						for (LocalContextListener l : contextOpened) {
							try {
								l.closeContext(executionConfig);
								Thread.currentThread().setContextClassLoader(classLoader);
							} catch (Exception e) {
								if (exception == null) {
									exception = e;
								} else {
									exception.addSuppressed(e);
								}
							}
						}
					} finally {
						contextOpened.clear();
					}
				}
				if (exception != null) {
					throw exception;
				}
			} finally {
				Thread.currentThread().setContextClassLoader(oldTCCL);
			}
		}
	}
}
