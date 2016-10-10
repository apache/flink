package org.apache.flink.api.java.io;

import static org.apache.flink.api.java.io.AvroOutputFormat.Codec;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.flink.api.io.avro.example.User;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.junit.Test;

/**
 * Tests for {@link AvroOutputFormat}
 */
public class AvroOutputFormatTest {

    @Test
    public void testSetCodec() throws Exception {
        // given
        final AvroOutputFormat<User> outputFormat = new AvroOutputFormat<>(User.class);

        // when
        try {
            outputFormat.setCodec(Codec.SNAPPY);
        } catch (Exception ex) {
            // then
            fail("unexpected exception");
        }
    }

    @Test
    public void testSetCodecError() throws Exception {
        // given
        boolean error = false;
        final AvroOutputFormat<User> outputFormat = new AvroOutputFormat<>(User.class);

        // when
        try {
            outputFormat.setCodec(null);
        } catch (Exception ex) {
            error = true;
        }

        // then
        assertTrue(error);
    }

    @Test
    public void testSerialization() throws Exception {

        serializeAndDeserialize(null);
        for (final Codec codec : Codec.values()) {
            serializeAndDeserialize(codec);
        }
    }

    private void serializeAndDeserialize(final Codec codec) throws IOException, ClassNotFoundException {
        // given
        final AvroOutputFormat<User> outputFormat = new AvroOutputFormat<>(User.class);
        if (codec != null) {
            outputFormat.setCodec(codec);
        }
        outputFormat.setSchema(User.SCHEMA$);

        final File serialized = File.createTempFile("avro-output-format", ".serialized");

        // when
        try (final ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(serialized))) {
            oos.writeObject(outputFormat);
        }
        try (final ObjectInputStream ois = new ObjectInputStream(new FileInputStream(serialized))) {
            // then
            assertTrue(ois.readObject() instanceof AvroOutputFormat);
        }

        // cleanup
        Files.delete(Paths.get(serialized.getPath()));
    }

    @Test
    public void testCompression() throws Exception {
        // given
        final Path outputPath = new Path(File.createTempFile("avro-output-file", "avro").getAbsolutePath());
        final AvroOutputFormat<User> outputFormat = new AvroOutputFormat<>(outputPath, User.class);
        outputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);

        final Path compressedOutputPath = new Path(File.createTempFile("avro-output-file", "compressed.avro").getAbsolutePath());
        final AvroOutputFormat<User> compressedOutputFormat = new AvroOutputFormat<>(compressedOutputPath, User.class);
        compressedOutputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
        compressedOutputFormat.setCodec(Codec.SNAPPY);

        // when
        output(outputFormat);
        output(compressedOutputFormat);

        // then
        assertTrue(fileSize(outputPath) > fileSize(compressedOutputPath));

        // cleanup
        Files.delete(Paths.get(outputPath.getPath()));
        Files.delete(Paths.get(compressedOutputPath.getPath()));
    }

    private long fileSize(Path path) throws IOException {
        return Files.size(Paths.get(path.getPath()));
    }

    private void output(final AvroOutputFormat<User> outputFormat) throws IOException {
        outputFormat.configure(new Configuration());
        outputFormat.open(1,1);
        for (int i = 0; i < 100; i++) {
            outputFormat.writeRecord(new User("testUser", 1, "blue"));
        }
        outputFormat.close();
    }
}
