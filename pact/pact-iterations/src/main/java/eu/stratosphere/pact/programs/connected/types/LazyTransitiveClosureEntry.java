package eu.stratosphere.pact.programs.connected.types;

import java.io.IOException;

import eu.stratosphere.nephele.services.memorymanager.SeekableDataInputView;
import eu.stratosphere.nephele.services.memorymanager.SeekableDataOutputView;
import eu.stratosphere.pact.runtime.iterative.LazyDeSerializable;


/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class LazyTransitiveClosureEntry extends TransitiveClosureEntry implements LazyDeSerializable {
  private SeekableDataInputView inView;
  private SeekableDataOutputView outView;

  private long pointer;

  private long[] neighborsCache;


  public LazyTransitiveClosureEntry()
  {
    this.vid = -1;
    this.cid = -1;
    this.numNeighbors = -1;
  }


  public long getVid()
  {
    if (this.vid == -1) {
      try {
        this.inView.setReadPosition(this.pointer);
        this.vid = this.inView.readLong();
      } catch (IOException ioex) {
        throw new RuntimeException(ioex);
      }
    }

    return this.vid;
  }

  public void setVid(long vid)
  {
    this.vid = vid;
    try {
      this.outView.setWritePosition(this.pointer);
      this.outView.writeLong(vid);
    } catch (IOException ioex) {
      throw new RuntimeException(ioex);
    }
  }

  public long getCid()
  {
    if (this.cid == -1) {
      try {
        this.inView.setReadPosition(this.pointer + 8);
        this.cid = this.inView.readLong();
      } catch (IOException ioex) {
        throw new RuntimeException(ioex);
      }
    }
    return cid;
  }

  public void setCid(long cid)
  {
    this.cid = cid;
    try {
      this.outView.setWritePosition(this.pointer + 8);
      this.outView.writeLong(cid);
    } catch (IOException ioex) {
      throw new RuntimeException(ioex);
    }
  }

  public long[] getNeighbors()
  {
    if (this.neighbors == null) {
      final int num = getNumNeighbors();

      if (this.neighborsCache == null || this.neighborsCache.length < num) {
        this.neighborsCache = new long[num];
      }

      long[] l = this.neighborsCache;
      try {
        this.inView.setReadPosition(this.pointer + 20);
        for (int i = 0; i < num; i++) {
          l[i] = this.inView.readLong();
        }
      } catch (IOException ioex) {
        throw new RuntimeException(ioex);
      }

      this.neighbors = l;
    }
    return this.neighbors;
  }

  public void setNeighbors(long[] neighbors, int num)
  {
    if (this.neighbors != null && num != this.numNeighbors) {
      throw new IllegalArgumentException("Cannot overwrite entry with array of different length.");
    }
    this.neighbors = neighbors;
    this.numNeighbors = num;

    try {
      this.outView.setWritePosition(this.pointer + 16);
      this.outView.writeInt(num);
      for (int i = 0; i < num; i++) {
        this.outView.writeLong(neighbors[i]);
      }
    } catch (IOException ioex) {
      throw new RuntimeException(ioex);
    }
  }

  public int getNumNeighbors()
  {
    if (this.numNeighbors == -1) {
      try {
        this.inView.setReadPosition(this.pointer + 16);
        this.numNeighbors = this.inView.readInt();
      } catch (IOException ioex) {
        throw new RuntimeException(ioex);
      }
    }
    return this.numNeighbors;
  }


  /* (non-Javadoc)
   * @see eu.stratosphere.pact.runtime.iterative.LazyDeSerializable#setDeserializer(eu.stratosphere.nephele.services.memorymanager.SeekableDataInputView, eu.stratosphere.nephele.services.memorymanager.SeekableDataOutputView, long)
   */
  @Override
  public void setDeserializer(SeekableDataInputView inView, SeekableDataOutputView outView, long startPosition) {
    this.inView = inView;
    this.outView = outView;
    this.pointer = startPosition;

    this.vid = -1;
    this.cid = -1;
    this.numNeighbors = -1;
    this.neighbors = null;
  }
}
