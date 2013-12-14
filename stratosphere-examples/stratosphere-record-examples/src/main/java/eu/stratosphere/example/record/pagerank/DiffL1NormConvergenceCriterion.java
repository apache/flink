package eu.stratosphere.example.record.pagerank;

import eu.stratosphere.api.functions.aggregators.ConvergenceCriterion;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DiffL1NormConvergenceCriterion implements ConvergenceCriterion<PageRankStats> {

	private static final double EPSILON = 0.00005;

	private static final Log log = LogFactory.getLog(DiffL1NormConvergenceCriterion.class);

	@Override
	public boolean isConverged(int iteration, PageRankStats pageRankStats) {
		double diff = pageRankStats.diff();

		if (log.isInfoEnabled()) {
			log.info("Stats in iteration [" + iteration + "]: " + pageRankStats);
			log.info("L1 norm of the vector difference is [" + diff + "] in iteration [" + iteration + "]");
		}

		return diff < EPSILON;
	}
}
