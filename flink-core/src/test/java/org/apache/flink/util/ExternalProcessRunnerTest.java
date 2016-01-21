/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ExternalProcessRunnerTest {

	@Test(expected = ClassNotFoundException.class)
	public void testClassNotFound() throws Exception {
		ExternalProcessRunner runner = new ExternalProcessRunner("MyClassThatDoesNotExist", new String[]{});
		runner.run();
	}

	@Test
	public void testInterrupting() throws Exception {

		final ExternalProcessRunner runner = new ExternalProcessRunner(InfiniteLoop.class.getName(), new String[]{});

		Thread thread = new Thread() {
			@Override
			public void run() {
				try {
					runner.run();
				} catch (InterruptedException e) {
					// this is expected
				} catch (Exception e) {
					fail("Other exception received " + e);
				}
			}
		};

		thread.start();
		thread.interrupt();
		thread.join();
	}

	@Test
	public void testPrintToErr() throws Exception {
		final ExternalProcessRunner runner = new ExternalProcessRunner(PrintToError.class.getName(), new String[]{"hello42"});

		int result = runner.run();

		assertEquals(0, result);
		assertEquals(runner.getErrorOutput().toString(), "Hello process hello42\n");
	}

	@Test
	public void testFailing() throws Exception {
		final ExternalProcessRunner runner = new ExternalProcessRunner(Failing.class.getName(), new String[]{});

		int result = runner.run();

		assertEquals(1, result);
		// this needs to be adapted if the test changes because it contains the line number
		assertEquals(runner.getErrorOutput().toString(), "Exception in thread \"main\" java.lang.RuntimeException: HEHE, I'm failing.\n" +
			"\tat org.apache.flink.util.ExternalProcessRunnerTest$Failing.main(ExternalProcessRunnerTest.java:94)\n");
	}


	public static class InfiniteLoop {
		public static void main(String[] args) {
			while (true) {
			}
		}
	}

	public static class PrintToError {
		public static void main(String[] args) {
			System.err.println("Hello process " + args[0]);
		}
	}

	public static class Failing {
		public static void main(String[] args) {
			throw new RuntimeException("HEHE, I'm failing.");
		}
	}

}
