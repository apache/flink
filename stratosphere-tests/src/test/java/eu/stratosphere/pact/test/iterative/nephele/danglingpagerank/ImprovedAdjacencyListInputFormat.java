package eu.stratosphere.pact.test.iterative.nephele.danglingpagerank;

import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;

public class ImprovedAdjacencyListInputFormat extends TextInputFormat {
  private static final long serialVersionUID = 1L;

  private final PactLong vertexID = new PactLong();
  private final AsciiLongArrayView arrayView = new AsciiLongArrayView();
  private final LongArrayView adjacentVertices = new LongArrayView();

  @Override
  public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes) {

    if (numBytes == 0) {
      return false;
    }

    arrayView.set(bytes, offset, numBytes);

    int numElements = arrayView.numElements();
    adjacentVertices.allocate(numElements - 1);

    try {

      int pos = 0;
      while (arrayView.next()) {

        if (pos == 0) {
          vertexID.setValue(arrayView.element());
        } else {
          adjacentVertices.setQuick(pos - 1, arrayView.element());
        }

        pos++;
      }

      //sanity check
      if (pos != numElements) {
        throw new IllegalStateException("Should have gotten " + numElements + " elements, but saw " + pos);
      }

    } catch (RuntimeException e) {
      throw new RuntimeException("Error parsing: " + arrayView.toString(), e);
    }

    target.clear();
    target.addField(vertexID);
    target.addField(adjacentVertices);

    return true;
  }
}

