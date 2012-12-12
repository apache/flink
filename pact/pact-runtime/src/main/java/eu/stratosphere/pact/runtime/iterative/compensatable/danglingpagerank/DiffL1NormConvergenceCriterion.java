package eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank;

import eu.stratosphere.pact.runtime.iterative.aggregate.Aggregator;
import eu.stratosphere.pact.runtime.iterative.convergence.ConvergenceCriterion;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DiffL1NormConvergenceCriterion implements ConvergenceCriterion<PageRankStats> {

  private static final double EPSILON = 0.00005;

  private static final Log log = LogFactory.getLog(DiffL1NormConvergenceCriterion.class);

  @Override
  public Aggregator<PageRankStats> createAggregator() {
    return new PageRankStatsAggregator();
  }

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
