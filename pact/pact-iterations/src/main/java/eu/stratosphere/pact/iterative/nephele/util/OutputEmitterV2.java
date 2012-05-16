package eu.stratosphere.pact.iterative.nephele.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.pact.common.generic.types.TypeSerializer;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

/**
* @author Erik Nijkamp
* @author Alexander Alexandrov
* @author Stephan Ewen
*/
@Deprecated
public class OutputEmitterV2 implements ChannelSelector<Value> {

    // ------------------------------------------------------------------------
    // Fields
    // ------------------------------------------------------------------------

    private static final byte[] DEFAULT_SALT = new byte[] { 17, 31, 47, 51, 83, 1 };
      
    private ShipStrategy strategy;        // the shipping strategy used by this output emitter
      
    private int[] channels;            // the reused array defining target channels
      
    private int nextChannelToSendTo = 0;    // counter to go over channels round robin
      
    private final byte[] salt;          // the salt used to randomize the hash values
      
    private JobID jobId;            // the job ID is necessary to obtain the class loader

//  private PartitionFunction partitionFunction;
      
    private TypeSerializer<Value> accessor;

    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    /**
     * Creates a new channel selector that uses the given strategy (broadcasting, partitioning, ...).
     * 
     * @param strategy
     *        The distribution strategy to be used.
     */
    public OutputEmitterV2(ShipStrategy strategy) {
        this.strategy = strategy;
        this.salt = DEFAULT_SALT;
    }  
      
    public OutputEmitterV2() {
        this.salt = DEFAULT_SALT;
    }
            
    public OutputEmitterV2(ShipStrategy strategy, JobID jobId, TypeSerializer<Value> accessor) {
        this(strategy, jobId, DEFAULT_SALT, accessor);
    }
      
    public OutputEmitterV2(ShipStrategy strategy, JobID jobId, byte[] salt, TypeSerializer<Value> accessor) {
        if (strategy == null | jobId == null | salt == null | accessor == null) { 
            throw new NullPointerException();
        }
        this.accessor = accessor;
        this.strategy = strategy;
        this.salt = salt;
        this.jobId = jobId;
    }

    // ------------------------------------------------------------------------
    // Channel Selection
    // ------------------------------------------------------------------------

    /*
     * (non-Javadoc)
     * @see eu.stratosphere.nephele.io.ChannelSelector#selectChannels(java.lang.Object, int)
     */
    @Override
    public final int[] selectChannels(Value record, int numberOfChannels) {
        switch (strategy) {
        case BROADCAST:
            return broadcast(numberOfChannels);
        case PARTITION_HASH:
        case PARTITION_LOCAL_HASH:
            return hashPartitionDefault(record, numberOfChannels);
        case FORWARD:
            return robin(numberOfChannels);
        case PARTITION_RANGE:
            System.arraycopy(usedChannels, 0, chan, 0, 4);
            return chan;
        default:
            throw new UnsupportedOperationException("Unsupported distribution strategy: " + strategy.name());
        }
    }
      
    int[] chan = new int[4];
    int[] usedChannels;
      
    public void setChannels(int[] usedChannels) {
        this.usedChannels = usedChannels;
    }
      
//  /**
//   * Set the partition function that is used for range partitioning
//   * @param func
//   */
//  public void setPartitionFunction(PartitionFunction func) {
//    this.partitionFunction = func;
//  }
      
    public void setTypeAccessor(TypeSerializer<Value> accessor) {
        this.accessor = accessor;
    }

//  private int[] partition_range(PactRecord record, int numberOfChannels) {
//    return partitionFunction.selectChannels(record, numberOfChannels);
//  }
      
    // --------------------------------------------------------------------------------------------

    private final int[] robin(int numberOfChannels) {
        if (this.channels == null || this.channels.length != 1) {
            this.channels = new int[1];
        }
            
        int nextChannel = nextChannelToSendTo + 1;
        nextChannel = nextChannel < numberOfChannels ? nextChannel : 0;
            
        this.nextChannelToSendTo = nextChannel;
        this.channels[0] = nextChannel;
        return this.channels;
    }

    private final int[] broadcast(int numberOfChannels) {
        if (channels == null || channels.length != numberOfChannels) {
            channels = new int[numberOfChannels];
            for (int i = 0; i < numberOfChannels; i++)
                channels[i] = i;
        }

        return channels;
    }

    private final int[] hashPartitionDefault(Value record, int numberOfChannels) {
        if (channels == null || channels.length != 1) {
            channels = new int[1];
        }
            
        int hash = accessor.hash(record);
            
        for (int i = 0; i < salt.length; i++) {
            hash ^= ((hash << 5) + salt[i] + (hash >> 2));
        }
      
        this.channels[0] = (hash < 0) ? -hash % numberOfChannels : hash % numberOfChannels;
        return this.channels;
    }

    // ------------------------------------------------------------------------
    // Serialization
    // ------------------------------------------------------------------------

    /*
     * (non-Javadoc)
     * @see eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
     */
    @Override
    public void read(DataInput in) throws IOException {
        // strategy
        this.strategy = ShipStrategy.valueOf(in.readUTF());
            
        // check whether further parameters come
        final boolean keyParameterized = in.readBoolean();
            
        if (keyParameterized) {
            // read the jobID to find the classloader
            this.jobId = new JobID();
            this.jobId.read(in);      
//      final ClassLoader loader = LibraryCacheManager.getClassLoader(this.jobId);
                  
            //String className = in.readUTF();
            //accessor = Class.forName(className, true, loader).asSubclass(Key.class).newInstance();
        }
    }

    /*
     * (non-Javadoc)
     * @see eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(strategy.name());
            
        if (this.accessor != null) {
            out.writeBoolean(true);
            this.jobId.write(out);
            //out.writeUTF(accessor.getClass().getName());
        }
        else {
            out.writeBoolean(false);
        }
    }
}
