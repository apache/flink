package org.apache.flink.mongodb.streaming.sink;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

public class DocumentBulkSerializer implements SimpleVersionedSerializer<DocumentBulk>, Serializable {

    private static final int MAGIC_NUMBER = 0x2f35a24b;

    private static final int END_OF_INPUT = -1;

    private static int version = 1;

    private static Codec<Document> DOCUMENT_CODEC = new DocumentCodec();

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public byte[] serialize(DocumentBulk documentBulk) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        out.writeInt(MAGIC_NUMBER);
        serializeV1(documentBulk, out);
        out.writeInt(END_OF_INPUT);
        System.out.println(out.toString());
        return out.getSharedBuffer();
    }

    @Override
    public DocumentBulk deserialize(int version, byte[] serialized) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        switch (version) {
            case 1:
                validateMagicNumber(in);
                return deserializeV1(in);
            default:
                throw new IOException("Unrecognized version or corrupt state: " + version);
        }
    }

    private void validateMagicNumber(DataInputView in) throws IOException {
        int magicNumber = in.readInt();
        if (magicNumber != MAGIC_NUMBER) {
            throw new IOException(
                    String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
        }
    }

    private void serializeV1(DocumentBulk documentBulk, DataOutputSerializer out) throws IOException {
        for (Document document : documentBulk.getDocuments()) {
            BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
            BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer);
            DOCUMENT_CODEC.encode(writer, document, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
            byte[] documentBytes = outputBuffer.toByteArray();
            out.writeInt(documentBytes.length);
            out.write(documentBytes);
        }
    }

    private DocumentBulk deserializeV1(DataInputDeserializer in) throws IOException {
        DocumentBulk bulk = new DocumentBulk();
        int len;
        while ((len = in.readInt()) != END_OF_INPUT) {
            byte[] docBuffer = new byte[len];
            in.read(docBuffer);
            BsonBinaryReader bsonReader = new BsonBinaryReader(ByteBuffer.wrap(docBuffer));
            bulk.add(DOCUMENT_CODEC.decode(bsonReader, DecoderContext.builder().build()));
        }
        return bulk;
    }
}
