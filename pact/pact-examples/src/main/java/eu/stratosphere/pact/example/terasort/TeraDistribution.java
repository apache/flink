/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.example.terasort;

import eu.stratosphere.pact.common.contract.DataDistribution;
import eu.stratosphere.pact.common.type.Key;

/**
 * This class implements a uniform data distribution for the TeraSort benchmark.
 * 
 * @author warneke
 */
public class TeraDistribution implements DataDistribution {

	@Override
	public Key getSplit(int splitId, int totalSplits) {

		final int fraction = 95 / (totalSplits+1);
		
		final byte b = (byte) (' ' + (fraction*(splitId+1)));
		final byte buf[] = new byte[TeraKey.KEY_SIZE];
		for(int i = 0; i < buf.length; ++i) {
			buf[i] = b;
		}
		
		return new TeraKey(buf);
	}

}
