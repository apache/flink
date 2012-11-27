/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.rpc;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TooManyListenersException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import eu.stratosphere.nephele.util.StringUtils;

public class RPCServiceTest implements RPCTestProtocol {

	private static final int RPC_TEST_PORT = 8000;

	private static final int NUMBER_OF_RPC_HANDLERS = 4;

	private static final int NUMBER_OF_TEST_STRINGS = 100;

	private static final int NUMBER_OF_RPC_CALLS = 1000;

	private final Set<Integer> processedRequestsSet = Collections
		.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());

	private final AtomicInteger counter = new AtomicInteger(0);

	private static List<Class<?>> getTypesToRegister() {

		final ArrayList<Class<?>> types = new ArrayList<Class<?>>();

		return types;
	}

	@Test
	public void testSingleClientRPCService() {

		this.counter.set(0);
		this.processedRequestsSet.clear();

		final List<String> par3 = new ArrayList<String>(NUMBER_OF_TEST_STRINGS);
		for (int i = 0; i < NUMBER_OF_TEST_STRINGS; ++i) {
			par3.add(constructTestString(i));
		}

		RPCService rpcService = null;
		try {
			rpcService = new RPCService(RPC_TEST_PORT, NUMBER_OF_RPC_HANDLERS, getTypesToRegister());
			rpcService.setProtocolCallbackHandler(RPCTestProtocol.class, this);

			final RPCTestProtocol proxy = rpcService.getProxy(new InetSocketAddress("localhost", RPC_TEST_PORT),
				RPCTestProtocol.class);

			for (int i = 0; i < NUMBER_OF_RPC_CALLS; ++i) {
				assertEquals(i, proxy.testMethod(true, i, par3));
			}

		} catch (Exception e) {
			fail(StringUtils.stringifyException(e));
		} finally {
			if (rpcService != null) {
				rpcService.shutDown();
			}
		}

		assertEquals(NUMBER_OF_RPC_CALLS, this.counter.get());
	}

	@Test
	public void testMultiClientRPCService() {

		this.counter.set(0);
		this.processedRequestsSet.clear();

		final List<String> par3 = new ArrayList<String>(NUMBER_OF_TEST_STRINGS);
		for (int i = 0; i < NUMBER_OF_TEST_STRINGS; ++i) {
			par3.add(constructTestString(i));
		}

		RPCService rpcService = null;
		try {
			rpcService = new RPCService(RPC_TEST_PORT, NUMBER_OF_RPC_HANDLERS, getTypesToRegister());
			rpcService.setProtocolCallbackHandler(RPCTestProtocol.class, this);

			final RPCTestProtocol proxy = rpcService.getProxy(new InetSocketAddress("localhost", RPC_TEST_PORT),
				RPCTestProtocol.class);

			final Thread[] threads = new Thread[NUMBER_OF_RPC_CALLS];

			for (int i = 0; i < NUMBER_OF_RPC_CALLS; ++i) {

				final int par2 = i;

				final Runnable runnable = new Runnable() {

					@Override
					public void run() {
						try {
							assertEquals(par2, proxy.testMethod(false, par2, par3));
						} catch (Exception e) {
							fail(StringUtils.stringifyException(e));
						}
					}
				};
				threads[i] = new Thread(runnable);
				threads[i].start();
			}

			for (int i = 0; i < NUMBER_OF_RPC_CALLS; ++i) {
				threads[i].join();
			}

		} catch (Exception e) {
			fail(StringUtils.stringifyException(e));
		} finally {
			if (rpcService != null) {
				rpcService.shutDown();
			}
		}
	}

	@Test
	public void testRegisteredThrowable() {

		RPCService rpcService = null;
		try {
			rpcService = new RPCService(RPC_TEST_PORT, NUMBER_OF_RPC_HANDLERS, getTypesToRegister());
			rpcService.setProtocolCallbackHandler(RPCTestProtocol.class, this);

			final RPCTestProtocol proxy = rpcService.getProxy(new InetSocketAddress("localhost", RPC_TEST_PORT),
				RPCTestProtocol.class);

			proxy.methodWithRegisteredThrowable();

		} catch (IOException e) {
			return;
		} catch (Exception e) {
			fail(StringUtils.stringifyException(e));
		} finally {
			if (rpcService != null) {
				rpcService.shutDown();
			}
		}

		fail("Expected IOException has not been caught");
	}

	@Test
	public void testUnregisteredThrowable() {

		RPCService rpcService = null;
		try {
			rpcService = new RPCService(RPC_TEST_PORT, NUMBER_OF_RPC_HANDLERS, getTypesToRegister());
			rpcService.setProtocolCallbackHandler(RPCTestProtocol.class, this);

			final RPCTestProtocol proxy = rpcService.getProxy(new InetSocketAddress("localhost", RPC_TEST_PORT),
				RPCTestProtocol.class);

			proxy.methodWithUnregisteredThrowable();

		} catch (IOException e) {
			return;
		} catch (Exception e) {
			fail(StringUtils.stringifyException(e));
		} finally {
			if (rpcService != null) {
				rpcService.shutDown();
			}
		}

		fail("Expected TooManyListenersException has not been caught");

	}

	private static String constructTestString(final int index) {

		final StringBuilder sb = new StringBuilder();
		for (int j = 0; j < index; ++j) {
			sb.append((char) (97 + (index % 26)));
		}

		return sb.toString();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int testMethod(final boolean par1, final int par2, final List<String> par3) {

		if (par1) {
			assertEquals(this.counter.getAndIncrement(), par2);
		}
		assertEquals(NUMBER_OF_TEST_STRINGS, par3.size());
		for (int i = 0; i < NUMBER_OF_TEST_STRINGS; ++i) {
			assertEquals(constructTestString(i), par3.get(i));
		}

		if (!this.processedRequestsSet.add(Integer.valueOf(par2))) {
			fail("Request with ID " + par2 + " is processed more than once");
		}

		return par2;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void methodWithRegisteredThrowable() throws IOException, InterruptedException {

		throw new IOException("This is an expected IOException");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void methodWithUnregisteredThrowable() throws IOException, InterruptedException, TooManyListenersException {

		throw new TooManyListenersException("This is an expected TooManyListenersException");
	}
}
