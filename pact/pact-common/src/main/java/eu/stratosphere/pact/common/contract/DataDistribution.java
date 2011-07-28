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

package eu.stratosphere.pact.common.contract;

import eu.stratosphere.pact.common.type.Key;

public interface DataDistribution {
	/**
	 * Generate split boarders. E.g. for splitId=0 generate the upper border of split.
	 * When there are n total splits the split ids from 0 - (n-1) will be generated as the
	 * last split is open ended.
	 * E.g. if totalSplits = 5, the following splitId's would be used
	 * 0,1,2,3,4
	 * And the partition would work this way:
	 * <===Key<0>===Key<1>===Key<2>===Key<3>===Key<4>==>
	 * So that both sides the lower and upper are open ended
	 * @param splitId
	 * @param totalSplits
	 * @return
	 */
	public Key getSplit(int splitId, int totalSplits); 
}
