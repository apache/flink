package com.fentik;

import java.io.UnsupportedEncodingException;

import org.apache.flink.table.functions.ScalarFunction;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

public class LZ4Compress extends ScalarFunction {
  private static int maxCompressedLength = 1024 * 16 - 2;

  public byte[] eval(String input) {
    LZ4Factory factory = LZ4Factory.fastestInstance();
    LZ4Compressor compressor = factory.fastCompressor();

    if (input == null) {
      return null;
    }

    byte[] inputBytes = null;
    try {
      inputBytes = input.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      return new byte[0];
    }
    byte[] compressed = new byte[maxCompressedLength + 2];

    int compressedLength = compressor.compress(inputBytes, 0, inputBytes.length, compressed, 0, maxCompressedLength);

    byte[] ret = new byte[compressedLength + 2];
    // XXX(sergei): this is a very naive header that allocates two bytes
    // at the start of every compressed buffer for the uncompressed length
    ret[0] = (byte) ((inputBytes.length >> 8) & 0xff);
    ret[1] = (byte) (inputBytes.length & 0xff);

    System.arraycopy(compressed, 0, ret, 2, compressedLength);
    return ret;
  }
}
