package eu.stratosphere.pact.programs.types;


/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class TransitiveClosureEntry {
  protected static final long[] EMPTY_NEIGHBORS = new long[0];

  protected long vid;

  protected long cid;

  protected long[] neighbors;

  protected int numNeighbors;


  public TransitiveClosureEntry()
  {
    this.neighbors = EMPTY_NEIGHBORS;
  }

  TransitiveClosureEntry(long vid, long cid, long[] neighbors) {
    this.vid = vid;
    this.cid = cid;
    this.neighbors = neighbors;
    this.numNeighbors = this.neighbors.length;
  }

  TransitiveClosureEntry(long vid, long cid, long[] neighbors, int numNeighbors) {
    this.vid = vid;
    this.cid = cid;
    this.neighbors = neighbors;
    this.numNeighbors = numNeighbors;
  }


  public long getVid() {
    return vid;
  }

  public void setVid(long vid) {
    this.vid = vid;
  }

  public long getCid() {
    return cid;
  }

  public void setCid(long cid) {
    this.cid = cid;
  }

  public long[] getNeighbors() {
    return neighbors;
  }

  public void setNeighbors(long[] neighbors, int num) {
    this.neighbors = neighbors;
    this.numNeighbors = num;
  }

  public int getNumNeighbors() {
    return this.numNeighbors;
  }
}
