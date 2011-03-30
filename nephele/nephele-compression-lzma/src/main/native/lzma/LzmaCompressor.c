#include "LzmaCompressor.h"
#include "lzma/LzmaLib.h"
#include "lzma/LzmaEnc.h"
#include "lzma/Types.h"
#include <stdlib.h>


/* A helper macro to 'throw' a java exception. */
#define THROW(env, exception_name, message) \
{ \
        jclass ecls = (*env)->FindClass(env, exception_name); \
        if (ecls) { \
          (*env)->ThrowNew(env, ecls, message); \
          (*env)->DeleteLocalRef(env, ecls); \
        } \
}

#define SIZE_LENGTH 8

static jfieldID LzmaCompressor_uncompressedDataBuffer;
static jfieldID LzmaCompressor_uncompressedDataBufferLength;
static jfieldID LzmaCompressor_compressedDataBuffer;
static jfieldID LzmaCompressor_compressedDataBufferLength;

JNIEXPORT void JNICALL Java_de_tu_1berlin_cit_nephele_io_compression_library_lzma_LzmaCompressor_initIDs (JNIEnv *env, jclass class){

	LzmaCompressor_uncompressedDataBuffer = (*env)->GetFieldID(env, class, "uncompressedDataBuffer", "Ljava/nio/ByteBuffer;");
	LzmaCompressor_uncompressedDataBufferLength = (*env)->GetFieldID(env, class, "uncompressedDataBufferLength", "I");
	LzmaCompressor_compressedDataBuffer = (*env)->GetFieldID(env, class, "compressedDataBuffer", "Ljava/nio/ByteBuffer;");
	LzmaCompressor_compressedDataBufferLength = (*env)->GetFieldID(env, class, "compressedDataBufferLength", "I");
	
}

JNIEXPORT jint JNICALL Java_de_tu_1berlin_cit_nephele_io_compression_library_lzma_LzmaCompressor_compressBytesDirect (JNIEnv *env, jobject this, jint offset){

        // Get members of LzoCompressor
        jobject uncompressed_buf = (*env)->GetObjectField(env, this, LzmaCompressor_uncompressedDataBuffer);
        jint uncompressed_buf_len = (*env)->GetIntField(env, this, LzmaCompressor_uncompressedDataBufferLength);

        jobject compressed_buf = (*env)->GetObjectField(env, this, LzmaCompressor_compressedDataBuffer);
	      jint compressed_buf_len = (*env)->GetIntField(env, this, LzmaCompressor_compressedDataBufferLength);
	      
	      // Get access to the uncompressed buffer object
	      const unsigned char *src = (*env)->GetDirectBufferAddress(env, uncompressed_buf);

	      if (src == 0)
		      return (jint)0;

	      // Get access to the compressed buffer object
        unsigned char *dest = (*env)->GetDirectBufferAddress(env, compressed_buf);

      	if (dest == 0)
              	return (jint)0;
              	
        SRes result;
        
        dest += offset;

        unsigned char *outProps = dest + SIZE_LENGTH;
        size_t outPropsSize = LZMA_PROPS_SIZE; 

	      size_t no_compressed_bytes = compressed_buf_len;
	      size_t no_uncompressed_bytes = uncompressed_buf_len;

	      // Compress
	      result = LzmaCompress(dest + SIZE_LENGTH + LZMA_PROPS_SIZE , &no_compressed_bytes, src, uncompressed_buf_len, outProps, &outPropsSize,
                        4, /* 0 <= level <= 9, default = 5 */
                        (1 << 18), /* use (1 << N) or (3 << N). 4 KB < dictSize <= 128 MB */
                        3, /* 0 <= lc <= 8, default = 3  */
                        0, /* 0 <= lp <= 4, default = 0  */
                        2, /* 0 <= pb <= 4, default = 2  */
                        32,  /* 5 <= fb <= 273, default = 32 */
                        1 /* numThreads 1 or 2, default = 2 */
                      );
                      
        //write length of compressed size to compressed buffer
        *(dest) = (unsigned char) ((no_compressed_bytes >> 24) & 0xff);
	      *(dest + 1) = (unsigned char) ((no_compressed_bytes >> 16) & 0xff);
	      *(dest + 2) = (unsigned char) ((no_compressed_bytes >> 8) & 0xff);
	      *(dest + 3) = (unsigned char) ((no_compressed_bytes >> 0) & 0xff);

	      //write length of uncompressed size to compressed buffer
        *(dest + 4) = (unsigned char) ((no_uncompressed_bytes >> 24) & 0xff);
	      *(dest + 5) = (unsigned char) ((no_uncompressed_bytes >> 16) & 0xff);
	      *(dest + 6) = (unsigned char) ((no_uncompressed_bytes >> 8) & 0xff);
	      *(dest + 7) = (unsigned char) ((no_uncompressed_bytes >> 0) & 0xff);
	
	
    if (result == SZ_OK) {
		// lzma compresses all input data
		  (*env)->SetIntField(env, this, LzmaCompressor_uncompressedDataBufferLength, 0);
	  } else {
		const int msg_len = 32;
		char exception_msg[msg_len];
		snprintf(exception_msg, msg_len, "LZMA-Compressor returned: %d", result);
		THROW(env, "java/lang/InternalError", exception_msg);
	}
        
  return (jint)no_compressed_bytes;        	
}
