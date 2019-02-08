package org.apache.flink.fs.gcp;

import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.fs.gcp.writer.GCSRecoverableWriter;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;

import java.io.IOException;

/**
 *
 */
public class GCSFileSystem extends HadoopFileSystem {
	//TODO: private fields for configuration

	public GCSFileSystem(org.apache.hadoop.fs.FileSystem hadoopFileSystem) {
		//TODO: receive configuration
		super(hadoopFileSystem);
	}

	@Override
	public FileSystemKind getKind() {
		return FileSystemKind.OBJECT_STORE;
	}

	@Override
	public RecoverableWriter createRecoverableWriter() throws IOException {
		//TODO: inject configuration
		return new GCSRecoverableWriter();
	}
}
