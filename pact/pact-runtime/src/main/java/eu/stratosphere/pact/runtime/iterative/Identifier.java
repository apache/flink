package eu.stratosphere.pact.runtime.iterative;

import com.google.common.base.Preconditions;
import eu.stratosphere.nephele.jobgraph.JobID;

/** Identifier object for a subtask */
public class Identifier {

  private final String id;

  public Identifier(JobID jobID, int subTaskID) {
    Preconditions.checkNotNull(jobID);
    id = jobID.toString() + "#" + subTaskID;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Identifier) {
      return id.equals(((Identifier) o).id);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public String toString() {
    return id;
  }
}
