package eu.stratosphere.pact.iterative.nephele.util;

import java.io.EOFException;
import java.io.IOException;

import eu.stratosphere.nephele.services.memorymanager.DataInputView;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.MutableObjectIterator;

@Deprecated
public class DeserializingIterator implements MutableObjectIterator<Value> {

  protected static final Log LOG = LogFactory.getLog(DeserializingIterator.class);

  private DataInputView input;

  public DeserializingIterator(DataInputView input) {
    this.input = input;
  }

  @Override
  public boolean next(Value target) throws IOException {
    try {
      target.read(input);
    } catch (EOFException ex) {
      return false;
    }
    return true;
  }
}