package eu.stratosphere.pact.runtime.iterative.playing.connectedcomponents;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.runtime.iterative.convergence.ConvergenceCriterion;

public class WorksetEmptyConvergenceCriterion implements ConvergenceCriterion {

  private boolean worksetEmpty = true;

  @Override
  public void prepareForNextIteration() {
    worksetEmpty = true;
  }

  @Override
  public void analyze(PactRecord record) {
    worksetEmpty = false;
  }

  @Override
  public boolean isConverged() {
    System.out.println("Is workset empty? " + worksetEmpty);
    return worksetEmpty;
  }
}
