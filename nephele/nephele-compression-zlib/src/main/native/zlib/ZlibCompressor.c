/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
#include "ZlibCompressor.h"
#include "zlib/zlib.h"
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

static jfieldID ZlibCompressor_uncompressedDataBuffer;
static jfieldID ZlibCompressor_uncompressedDataBufferLength;
static jfieldID ZlibCompressor_compressedDataBuffer;
static jfieldID ZlibCompressor_compressedDataBufferLength;

JNIEXPORT void JNICALL Java_de_tu_1berlin_cit_nephele_io_compression_library_zlib_ZlibCompressor_initIDs (JNIEnv *env, jclass class){

	ZlibCompressor_uncompressedDataBuffer = (*env)->GetFieldID(env, class, "uncompressedDataBuffer", "Ljava/nio/ByteBuffer;");
	ZlibCompressor_uncompressedDataBufferLength = (*env)->GetFieldID(env, class, "uncompressedDataBufferLength", "I");
	ZlibCompressor_compressedDataBuffer = (*env)->GetFieldID(env, class, "compressedDataBuffer", "Ljava/nio/ByteBuffer;");
	ZlibCompressor_compressedDataBufferLength = (*env)->GetFieldID(env, class, "compressedDataBufferLength", "I");
	
}

JNIEXPORT jint JNICALL Java_de_tu_1berlin_cit_nephele_io_compression_library_zlib_ZlibCompressor_compressBytesDirect (JNIEnv *env, jobject this, jint offset){
                     
        // Get members of LzoCompressor
        jobject uncompressed_buf = (*env)->GetObjectField(env, this, ZlibCompressor_uncompressedDataBuffer);
        jint uncompressed_buf_len = (*env)->GetIntField(env, this, ZlibCompressor_uncompressedDataBufferLength);

        jobject compressed_buf = (*env)->GetObjectField(env, this, ZlibCompressor_compressedDataBuffer);
	      jint compressed_buf_len = (*env)->GetIntField(env, this, ZlibCompressor_compressedDataBufferLength);
	      
	      // Get access to the uncompressed buffer object
	      const unsigned char *src = (*env)->GetDirectBufferAddress(env, uncompressed_buf);

	      if (src == 0){
          const int msg_len = 64;
		      char exception_msg[msg_len];
		      snprintf(exception_msg, msg_len, "Zlib-Compressor - No access to uncompressed-buffer");
		      THROW(env, "java/lang/InternalError", exception_msg);
		      return (jint)0;
        }
		    //  return (jint)0;

	      // Get access to the compressed buffer object
        unsigned char *dest = (*env)->GetDirectBufferAddress(env, compressed_buf);

      	if (dest == 0){
		      const int msg_len = 64;
		      char exception_msg[msg_len];
		      snprintf(exception_msg, msg_len, "Zlib-Compressor - no access to compressed-buffer");
		      THROW(env, "java/lang/InternalError", exception_msg);
		      return (jint)0;
		      
        }
              	//return (jint)0;
              	
        dest += offset;
              	
        unsigned long no_compressed_bytes = compressed_buf_len;
        unsigned long no_uncompressed_bytes = uncompressed_buf_len;
        
        int result = compress(dest + SIZE_LENGTH, 
                              &no_compressed_bytes,
                              src, 
                              no_uncompressed_bytes
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
                                            
        if (result == Z_OK) {
		      // Zlib compresses all input data
		      (*env)->SetIntField(env, this, ZlibCompressor_uncompressedDataBufferLength, 0);
	     } else {
		      const int msg_len = 32;
		      char exception_msg[msg_len];
		      snprintf(exception_msg, msg_len, "Zlib-Compressor returned: %d", result);
		      THROW(env, "java/lang/InternalError", exception_msg);
     	}
     	
     	return (jint)no_compressed_bytes;

}
