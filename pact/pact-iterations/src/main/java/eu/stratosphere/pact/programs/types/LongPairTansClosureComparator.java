package eu.stratosphere.pact.programs.types;

import eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2;
import eu.stratosphere.pact.runtime.plugable.TypeComparator;


/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class LongPairTansClosureComparator implements TypeComparator<LongPair, TransitiveClosureEntry> {
  private long referenceKey;

  /* (non-Javadoc)
   * @see eu.stratosphere.pact.runtime.plugable.TypeComparator#setFirstAsReference(java.lang.Object, eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2)
   */
  @Override
  public void setReference(LongPair reference, TypeAccessorsV2<LongPair> accessor)
  {
    this.referenceKey = reference.getKey();
  }

  /* (non-Javadoc)
   * @see eu.stratosphere.pact.runtime.plugable.TypeComparator#equalsSecondToReference(java.lang.Object, eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2)
   */
  @Override
  public boolean equalToReference(TransitiveClosureEntry candidate, TypeAccessorsV2<TransitiveClosureEntry> accessor)
  {
    return this.referenceKey == candidate.getVid();
  }

}
