package eu.stratosphere.nephele.rpc;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import eu.stratosphere.nephele.util.StringUtils;

public class RPCServiceTest implements RPCTestProtocol {

	private static final int RPC_TEST_PORT = 8000;

	private static final int NUMBER_OF_TEST_STRINGS = 100;

	private static final int NUMBER_OF_RPC_CALLS = 100;

	private final AtomicInteger counter = new AtomicInteger(0);

	@Test
	public void testSingleClientRPCService() {

		this.counter.set(0);

		final List<String> par3 = new ArrayList<String>(NUMBER_OF_TEST_STRINGS);
		for (int i = 0; i < NUMBER_OF_TEST_STRINGS; ++i) {
			par3.add(constructTestString(i));
		}

		RPCService rpcService = null;
		try {
			rpcService = new RPCService(RPC_TEST_PORT);
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

		final List<String> par3 = new ArrayList<String>(NUMBER_OF_TEST_STRINGS);
		for (int i = 0; i < NUMBER_OF_TEST_STRINGS; ++i) {
			par3.add(constructTestString(i));
		}

		RPCService rpcService = null;
		try {
			rpcService = new RPCService(RPC_TEST_PORT);
			rpcService.setProtocolCallbackHandler(RPCTestProtocol.class, this);

			final RPCTestProtocol proxy = rpcService.getProxy(new InetSocketAddress("localhost", RPC_TEST_PORT),
				RPCTestProtocol.class);

			final Thread[] threads = new Thread[NUMBER_OF_RPC_CALLS];

			for (int i = 0; i < NUMBER_OF_RPC_CALLS; ++i) {

				final int par2 = i;

				final Runnable runnable = new Runnable() {

					@Override
					public void run() {
						assertEquals(par2, proxy.testMethod(false, par2, par3));
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

	private static String constructTestString(final int index) {

		final StringBuilder sb = new StringBuilder();
		for (int j = 0; j < index; ++j) {
			sb.append((char) (97 + (index % 26)));
		}

		return sb.toString();
	}

	@Override
	public int testMethod(final boolean par1, final int par2, final List<String> par3) {

		if (par1) {
			assertEquals(this.counter.getAndIncrement(), par2);
		}
		assertEquals(NUMBER_OF_TEST_STRINGS, par3.size());
		for (int i = 0; i < NUMBER_OF_TEST_STRINGS; ++i) {
			assertEquals(constructTestString(i), par3.get(i));
		}

		return par2;
	}
}
