package eu.stratosphere.nephele.executiongraph;

import eu.stratosphere.nephele.io.DistributionPattern;

public class DistributionPatternProvider {

	/**
	 * Checks if two subtasks of different tasks should be wired.
	 * 
	 * @param pattern
	 *        the distribution pattern that sould be used.
	 * @param nodeLowerStage
	 *        the index of the producing task's subtask
	 * @param nodeUpperStage
	 *        the index of the consuming task's subtask
	 * @param sizeSetLowerStage
	 *        the number of subtasks of the producing task
	 * @param sizeSetUpperStage
	 *        the number of subtasks of the consuming task
	 * @return <code>true</code> if a wire between the two considered subtasks should be created, <code>false</code>
	 *         otherwise
	 */
	public static synchronized boolean createWire(DistributionPattern pattern, int nodeLowerStage, int nodeUpperStage,
			int sizeSetLowerStage, int sizeSetUpperStage) {

		switch (pattern) {
		case BIPARTITE:
			return true;

		case POINTWISE:
			if (sizeSetLowerStage < sizeSetUpperStage) {
				if (nodeLowerStage == (nodeUpperStage % sizeSetLowerStage)) {
					return true;
				}
			} else {
				if ((nodeLowerStage % sizeSetUpperStage) == nodeUpperStage) {
					return true;
				}
			}

			return false;

		case STAR:
			if (sizeSetLowerStage > sizeSetUpperStage) {

				int groupNumber = nodeLowerStage / Math.max(sizeSetLowerStage / sizeSetUpperStage, 1);

				if (nodeUpperStage == groupNumber) {
					return true;
				}
			} else {

				int groupNumber = nodeUpperStage / Math.max(sizeSetUpperStage / sizeSetLowerStage, 1);

				if (nodeLowerStage == groupNumber) {
					return true;
				}

			}

			return false;

		default:
			// this will never happen.
			throw new IllegalStateException("No Match for Distribution Pattern found.");
		}
	}
}
