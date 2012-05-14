package eu.stratosphere.pact.iterative.nephele.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;

public class OutputCollectorV2 {
  // list of writers
  protected RecordWriter<Value>[] writers;

  /**
   * Initializes the output collector with no writers.
   */
  public OutputCollectorV2() {
  }

  /**
   * Adds a writer to the OutputCollector.
   *
   * @param writer The writer to add.
   */
  @SuppressWarnings("unchecked")
  public void addWriter(RecordWriter<Value> writer)
  {
    // avoid using the array-list here to reduce one level of object indirection
    if (this.writers == null) {
      this.writers = new RecordWriter[] {writer};
    }
    else {
      RecordWriter<Value>[] ws = new RecordWriter[this.writers.length + 1];
      System.arraycopy(this.writers, 0, ws, 0, this.writers.length);
      ws[this.writers.length] = writer;
      this.writers = ws;
    }
  }

  /**
   * Collects a {@link PactRecord}, and emits it to all writers.
   * Writers which require a deep-copy are fed with a copy.
   */
  public void collect(Value record)
  {
    try {
      for (int i = 0; i < writers.length; i++) {
        this.writers[i].emit(record);
      }
    }
    catch (IOException e) {
      throw new RuntimeException("Emitting the record caused an I/O exception: " + e.getMessage(), e);
    }
    catch (InterruptedException e) {
      throw new RuntimeException("Emitting the record was interrupted: " + e.getMessage(), e);
    }
  }

  /*
   * (non-Javadoc)
   * @see eu.stratosphere.pact.common.stub.Collector#close()
   */
  public void close() {
  }

  /**
   * List of writers that are associated with this output collector
   * @return list of writers
   */
  public List<RecordWriter<Value>> getWriters() {
    return Collections.unmodifiableList(Arrays.asList(writers));
  }
}