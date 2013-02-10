package eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank;

import com.google.common.base.Charsets;

public class AsciiLongArrayView {

  private byte[] buffer;

  private int offset;
  private int numBytes;

  private int tokenOffset;
  private int tokenNumBytes;

  private static final int NOT_SET = -1;

  private static final int RADIX_TEN = 10;
  private static final long MULTMIN_RADIX_TEN = Long.MIN_VALUE / 10;
  private static final long N_MULTMAX_RADIX_TEN = -Long.MAX_VALUE / 10;

  public void set(byte[] buffer, int offset, int numBytes) {
    this.buffer = buffer;
    this.offset = offset;
    this.numBytes = numBytes;

    this.tokenOffset = NOT_SET;
    checkForSingleTrailingWhitespace();
  }

  private void checkForSingleTrailingWhitespace() {
    if (Character.isWhitespace((char) buffer[offset + numBytes -1])) {
      numBytes--;
    }
  }

  public int numElements() {
    int matches = 0;
    int pos = offset;
    while (pos < offset + numBytes) {
      if (Character.isWhitespace((char) buffer[pos])) {
        matches++;
      }
      pos++;
    }
    return matches + 1;
  }

  public boolean next() {

    if (tokenOffset == NOT_SET) {
      tokenOffset = offset;
    } else {
      tokenOffset += tokenNumBytes + 1;
      if (tokenOffset > offset + numBytes) {
        return false;
      }
    }

    tokenNumBytes = 1;
    while (true) {
      int candidatePos = tokenOffset + tokenNumBytes;
      if (candidatePos >= offset + numBytes || Character.isWhitespace((char) buffer[candidatePos])) {
        break;
      }
      tokenNumBytes++;
    }

    return true;
  }

  private char tokenCharAt(int pos) {
    return (char) buffer[tokenOffset + pos];
  }

  public long element() {

    long result = 0;
    boolean negative = false;
    int i = 0, max = tokenNumBytes;
    long limit;
    long multmin;
    int digit;

    if (max > 0) {
      if (tokenCharAt(0) == '-') {
        negative = true;
        limit = Long.MIN_VALUE;
        i++;
      } else {
        limit = -Long.MAX_VALUE;
      }

      multmin = negative ? MULTMIN_RADIX_TEN : N_MULTMAX_RADIX_TEN;

      if (i < max) {
        digit = Character.digit(tokenCharAt(i++), RADIX_TEN);
        if (digit < 0) {
          throw new NumberFormatException(toString());
        } else {
          result = -digit;
        }
      }
      while (i < max) {
        // Accumulating negatively avoids surprises near MAX_VALUE
        digit = Character.digit(tokenCharAt(i++), RADIX_TEN);
        if (digit < 0) {
          throw new NumberFormatException(toString());
        }
        if (result < multmin) {
          throw new NumberFormatException(toString());
        }
        result *= RADIX_TEN;
        if (result < limit + digit) {
          throw new NumberFormatException(toString());
        }
        result -= digit;
      }
    } else {
      throw new NumberFormatException(toString());
    }
    if (negative) {
      if (i > 1) {
        return result;
      } else { /* Only got "-" */
        throw new NumberFormatException(toString());
      }
    } else {
      return -result;
    }
  }

  public double elementAsDouble() {
    String token = new String(buffer, tokenOffset, tokenNumBytes, Charsets.US_ASCII);
    return Double.valueOf(token);
  }


  @Override
  public String toString() {
    return "[" + new String(buffer, offset, numBytes, Charsets.US_ASCII) + "] (buffer length: " + buffer.length +
        ", offset: " + offset + ", numBytes: " + numBytes + ")";
  }

}
