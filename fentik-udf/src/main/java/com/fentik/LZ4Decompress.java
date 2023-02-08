package com.fentik;

import org.apache.flink.table.functions.ScalarFunction;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4Factory;

public class LZ4Decompress extends ScalarFunction {
  public String eval(byte[] compressed) {
    LZ4Factory factory = LZ4Factory.fastestInstance();
    LZ4FastDecompressor decompressor = factory.fastDecompressor();

    if (compressed == null) {
      return null;
    }

    // XXX(sergei): this is a very naive header that allocates two bytes
    // at the start of every compressed buffer for the uncompressed length
    int decompressedLength = (int) compressed[0] << 16 | (int) compressed[1];
    byte[] restored = new byte[decompressedLength];
    decompressor.decompress(compressed, 2, restored, 0, decompressedLength);
    return new String(restored);
  }
}
