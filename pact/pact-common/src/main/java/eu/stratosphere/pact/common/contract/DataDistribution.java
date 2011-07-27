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
