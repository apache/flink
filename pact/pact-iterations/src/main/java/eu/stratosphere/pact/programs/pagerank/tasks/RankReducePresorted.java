package eu.stratosphere.pact.programs.pagerank.tasks;

import eu.stratosphere.pact.iterative.nephele.tasks.AbstractIterativeTask;
import eu.stratosphere.pact.iterative.nephele.util.IterationIterator;
import eu.stratosphere.pact.programs.pagerank.types.VertexPageRank;
import eu.stratosphere.pact.programs.pagerank.types.VertexPageRankAccessor;

public class RankReducePresorted extends AbstractIterativeTask {

  @Override
  protected void initTask() {
    outputAccessors[0] = new VertexPageRankAccessor();
  }

  @Override
  public void runIteration(IterationIterator iterationIter) throws Exception {
    VertexPageRank contrib = new VertexPageRank();
    VertexPageRank result = new VertexPageRank();
    boolean firstKey = true;
    long referenceKey = 0;

    double contribSum = 0;
    double rank = 0;
    while (iterationIter.next(contrib)) {
      long key = contrib.getVid();

      if (firstKey || key != referenceKey) {
        if (firstKey) {
          firstKey = false;
        } else {
          //Send previous key with page rank
          rank = 0.15 / 14052 + 0.85 * contribSum;
          if (key == 83760334) {
            LOG.info("Wolke: " + contribSum);
          }
          if (Double.isNaN(rank)) {
            LOG.info("NAN: "  + referenceKey + "::" + rank + " // " + contribSum );
          }
          result.setVid(referenceKey);
          result.setRank(rank);
          output.collect(result);
        }
        //Set new page rank key and initial contribSum
        contribSum = contrib.getRank();
        referenceKey = key;
      } else {
        contribSum += contrib.getRank();
      }
    }

    if (!firstKey) {
      rank = 0.15 / 14052 + 0.85 * contribSum;
      if (Double.isNaN(rank)) {
        LOG.info("NAN: "  + referenceKey + "::" + rank + " // " + contribSum );
      }
      result.setVid(referenceKey);
      result.setRank(rank);
      output.collect(result);
    }
  }

  @Override
  public int getNumberOfInputs() {
    return 1;
  }

  @Override
  public void cleanup() throws Exception {
  }
}
