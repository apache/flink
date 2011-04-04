/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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
#include "Bzip2Compressor.h"
#include "bzip2/bzlib.h"
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

static jfieldID Bzip2Compressor_uncompressedDataBuffer;
static jfieldID Bzip2Compressor_uncompressedDataBufferLength;
static jfieldID Bzip2Compressor_compressedDataBuffer;
static jfieldID Bzip2Compressor_compressedDataBufferLength;

JNIEXPORT void JNICALL Java_de_tu_1berlin_cit_nephele_io_compression_library_bzip2_Bzip2Compressor_initIDs (JNIEnv *env, jclass class){

	Bzip2Compressor_uncompressedDataBuffer = (*env)->GetFieldID(env, class, "uncompressedDataBuffer", "Ljava/nio/ByteBuffer;");
	Bzip2Compressor_uncompressedDataBufferLength = (*env)->GetFieldID(env, class, "uncompressedDataBufferLength", "I");
	Bzip2Compressor_compressedDataBuffer = (*env)->GetFieldID(env, class, "compressedDataBuffer", "Ljava/nio/ByteBuffer;");
	Bzip2Compressor_compressedDataBufferLength = (*env)->GetFieldID(env, class, "compressedDataBufferLength", "I");
	
}

JNIEXPORT jint JNICALL Java_de_tu_1berlin_cit_nephele_io_compression_library_bzip2_Bzip2Compressor_compressBytesDirect (JNIEnv *env, jobject this, jint offset){

        // Get members of Bzip2Compressor
        jobject uncompressed_buf = (*env)->GetObjectField(env, this, Bzip2Compressor_uncompressedDataBuffer);
        jint uncompressed_buf_len = (*env)->GetIntField(env, this, Bzip2Compressor_uncompressedDataBufferLength);

        jobject compressed_buf = (*env)->GetObjectField(env, this, Bzip2Compressor_compressedDataBuffer);
	      jint compressed_buf_len = (*env)->GetIntField(env, this, Bzip2Compressor_compressedDataBufferLength);
	      
	      // Get access to the uncompressed buffer object
	      char *src = (*env)->GetDirectBufferAddress(env, uncompressed_buf);

	      if (src == 0)
		      return (jint)0;

	      // Get access to the compressed buffer object
        char *dest = (*env)->GetDirectBufferAddress(env, compressed_buf);

      	if (dest == 0)
              	return (jint)0;
              	
        dest += offset;
              	
        unsigned int no_compressed_bytes = compressed_buf_len;
        unsigned int no_uncompressed_bytes = uncompressed_buf_len;
        
        //compress
        int result = BZ2_bzBuffToBuffCompress( 
                                            dest + SIZE_LENGTH, 
                                            &no_compressed_bytes,
                                            src, 
                                            no_uncompressed_bytes,
                                            5, 
                                            0, 
                                            30 
                                            );
                                            
        //write length of compressed size to compressed buffer
        *(dest) = (char) ((no_compressed_bytes >> 24) & 0xff);
	      *(dest + 1) = (char) ((no_compressed_bytes >> 16) & 0xff);
	      *(dest + 2) = (char) ((no_compressed_bytes >> 8) & 0xff);
	      *(dest + 3) = (char) ((no_compressed_bytes >> 0) & 0xff);

	      //write length of uncompressed size to compressed buffer
        *(dest + 4) = (char) ((no_uncompressed_bytes >> 24) & 0xff);
	      *(dest + 5) = (char) ((no_uncompressed_bytes >> 16) & 0xff);
	      *(dest + 6) = (char) ((no_uncompressed_bytes >> 8) & 0xff);
	      *(dest + 7) = (char) ((no_uncompressed_bytes >> 0) & 0xff);
                                            
        if (result == BZ_OK) {
		      // bzip2 compresses all input data
		      (*env)->SetIntField(env, this, Bzip2Compressor_uncompressedDataBufferLength, 0);
	     } else {
		      const int msg_len = 32;
		      char exception_msg[msg_len];
		      snprintf(exception_msg, msg_len, "Bzip2-Compressor returned: %d", result);
		      THROW(env, "java/lang/InternalError", exception_msg);
     	}
     	
     	return (jint)no_compressed_bytes;

}
