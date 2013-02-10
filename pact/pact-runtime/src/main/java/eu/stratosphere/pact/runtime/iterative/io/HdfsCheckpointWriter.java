package eu.stratosphere.pact.runtime.iterative.io;

import com.google.common.io.Closeables;
import eu.stratosphere.pact.common.type.PactRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.Closeable;
import java.io.IOException;

public abstract class HdfsCheckpointWriter implements Closeable {

  private SequenceFile.Writer writer;

  public HdfsCheckpointWriter() {}

  protected abstract Class<? extends WritableComparable> keyClass();
  protected abstract Class<? extends Writable> valueClass();

  public void open(String path) throws IOException {
    Path hdfsPath = new Path(path);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(hdfsPath.toUri(), conf);
    writer = new SequenceFile.Writer(fs, conf, hdfsPath, keyClass(), valueClass());
  }

  public void addToCheckpoint(PactRecord record) throws IOException {
    add(writer, record);
  }

  protected abstract void add(SequenceFile.Writer writer, PactRecord record) throws IOException;

  @Override
  public void close() throws IOException {
    Closeables.closeQuietly(writer);
  }
}
