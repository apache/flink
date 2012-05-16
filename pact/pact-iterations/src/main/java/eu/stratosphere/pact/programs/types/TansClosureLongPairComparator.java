package eu.stratosphere.pact.programs.types;

import eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2;
import eu.stratosphere.pact.runtime.plugable.TypeComparator;


/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class TansClosureLongPairComparator implements TypeComparator<TransitiveClosureEntry, LongPair> {
  private long referenceKey;

  /* (non-Javadoc)
   * @see eu.stratosphere.pact.runtime.plugable.TypeComparator#setFirstAsReference(java.lang.Object, eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2)
   */
  @Override
  public void setReference(TransitiveClosureEntry reference, TypeAccessorsV2<TransitiveClosureEntry> accessor)
  {
    this.referenceKey = reference.getVid();
  }

  /* (non-Javadoc)
   * @see eu.stratosphere.pact.runtime.plugable.TypeComparator#equalsSecondToReference(java.lang.Object, eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2)
   */
  @Override
  public boolean equalToReference(LongPair candidate, TypeAccessorsV2<LongPair> accessor)
  {
    return this.referenceKey == candidate.getKey();
  }

}
