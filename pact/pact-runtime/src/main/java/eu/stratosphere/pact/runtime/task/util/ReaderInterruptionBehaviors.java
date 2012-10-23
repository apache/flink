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

package eu.stratosphere.pact.runtime.task.util;

import java.io.IOException;

public class ReaderInterruptionBehaviors {

	public static final ReaderInterruptionBehavior EXCEPTION_ON_INTERRUPT = new ReaderInterruptionBehavior() {
		@Override
		public boolean onInterrupt(InterruptedException e) throws IOException {
			throw new IOException("Reader was interrupted.", e);
		}
	};

	public static final ReaderInterruptionBehavior FALSE_ON_INTERRUPT = new ReaderInterruptionBehavior() {
		@Override
		public boolean onInterrupt(InterruptedException e) throws IOException {
			return false;
		}
	};
	
	// --------------------------------------------------------------------------------------------
	
	private ReaderInterruptionBehaviors() {}
}
