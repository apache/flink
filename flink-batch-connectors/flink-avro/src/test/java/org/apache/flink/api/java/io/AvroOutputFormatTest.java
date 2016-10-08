package org.apache.flink.api.java.io;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.avro.file.CodecFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.junit.Test;

/**
 * Tests for {@link AvroOutputFormat}
 */
public class AvroOutputFormatTest {

    @Test
    public void testSetCodecFactory() throws Exception {
        // given
        final AvroOutputFormat<DummyAvroType> outputFormat = new AvroOutputFormat<>(DummyAvroType.class);

        // when
        try {
            outputFormat.setCodecFactory(CodecFactory.snappyCodec());
        } catch (Exception ex) {
            // then
            fail("unexpected exception");
        }
    }

    @Test
    public void testSetCodecFactoryError() throws Exception {
        // given
        boolean error = false;
        final AvroOutputFormat<DummyAvroType> outputFormat = new AvroOutputFormat<>(DummyAvroType.class);

        // when
        try {
            outputFormat.setCodecFactory(null);
        } catch (Exception ex) {
            error = true;
        }

        // then
        assertTrue(error);
    }

    @Test
    public void testCompression() throws Exception {
        // given
        final Path outputPath = path("avro-output-file.avro");
        final AvroOutputFormat<DummyAvroType> outputFormat = new AvroOutputFormat<>(outputPath, DummyAvroType.class);
        outputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);

        final Path compressedOutputPath = path("avro-output-file-compressed.avro");
        final AvroOutputFormat<DummyAvroType> compressedOutputFormat = new AvroOutputFormat<>(compressedOutputPath, DummyAvroType.class);
        compressedOutputFormat.setWriteMode(FileSystem.WriteMode.OVERWRITE);
        compressedOutputFormat.setCodecFactory(CodecFactory.snappyCodec());

        // when
        output(outputFormat);
        output(compressedOutputFormat);

        // then
        assertTrue(fileSize(outputPath) > fileSize(compressedOutputPath));
    }

    private long fileSize(Path path) throws IOException {
        return Files.size(Paths.get(path.getPath()));
    }

    private void output(final AvroOutputFormat<DummyAvroType> outputFormat) throws IOException {
        outputFormat.configure(new Configuration());
        outputFormat.open(1,1);
        for (int i = 0; i < 100; i++) {
            outputFormat.writeRecord(new DummyAvroType(1));
        }
        outputFormat.close();
    }

    private Path path(final String virtualPath) throws URISyntaxException {
        return new Path(Paths.get(getClass().getResource("/").toURI()).toString() + "/" + virtualPath);
    }

    private static class DummyAvroType {

        private int id;

        public DummyAvroType(final int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }

        public void setId(final int id) {
            this.id = id;
        }
    }
}
