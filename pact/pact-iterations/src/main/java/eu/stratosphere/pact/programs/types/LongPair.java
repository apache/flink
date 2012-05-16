package eu.stratosphere.pact.programs.types;


/**
 * 
 */
public class LongPair {
  private long key;
  private long value;


  public LongPair()
  {}

  public LongPair(long key, long value)
  {
    this.key = key;
    this.value = value;
  }


  public long getKey() {
    return key;
  }

  public void setKey(long key) {
    this.key = key;
  }

  public long getValue() {
    return value;
  }

  public void setValue(long value) {
    this.value = value;
  }
}
