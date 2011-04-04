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

#include "ZlibDecompressor.h"
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
/*z_stream *configData;
unsigned char *dest;
unsigned char *src;
jint uncompressed_buf_len;
jint compressed_buf_len;
  */
static jfieldID ZlibDecompressor_uncompressedDataBuffer;
static jfieldID ZlibDecompressor_uncompressedDataBufferLength;
static jfieldID ZlibDecompressor_compressedDataBuffer;
static jfieldID ZlibDecompressor_compressedDataBufferLength;

JNIEXPORT void JNICALL Java_de_tu_1berlin_cit_nephele_io_compression_library_zlib_ZlibDecompressor_initIDs (JNIEnv *env, jclass class){

	ZlibDecompressor_uncompressedDataBuffer = (*env)->GetFieldID(env, class, "uncompressedDataBuffer", "Ljava/nio/ByteBuffer;");
	ZlibDecompressor_uncompressedDataBufferLength = (*env)->GetFieldID(env, class, "uncompressedDataBufferLength", "I");
	ZlibDecompressor_compressedDataBuffer = (*env)->GetFieldID(env, class, "compressedDataBuffer", "Ljava/nio/ByteBuffer;");
	ZlibDecompressor_compressedDataBufferLength = (*env)->GetFieldID(env, class, "compressedDataBufferLength", "I");
	
}

JNIEXPORT jint JNICALL Java_de_tu_1berlin_cit_nephele_io_compression_library_zlib_ZlibDecompressor_decompressBytesDirect (JNIEnv *env, jobject this, jint offset){
                                                                                                                    
        // Get members of ZlibDecompressor  
        jobject uncompressed_buf = (*env)->GetObjectField(env, this, ZlibDecompressor_uncompressedDataBuffer);
        jobject compressed_buf = (*env)->GetObjectField(env, this, ZlibDecompressor_compressedDataBuffer);
        
	unsigned char *src = (*env)->GetDirectBufferAddress(env, compressed_buf);
        unsigned char *dest = (*env)->GetDirectBufferAddress(env, uncompressed_buf);
	
	jint compressed_buf_len = (*env)->GetIntField(env, this, ZlibDecompressor_compressedDataBufferLength);
	jint uncompressed_buf_len = (*env)->GetIntField(env, this, ZlibDecompressor_uncompressedDataBufferLength);

	uLongf number_of_uncompressed_bytes = uncompressed_buf_len;

	int result = uncompress(dest, &number_of_uncompressed_bytes, src+offset, (uLong) compressed_buf_len-offset);
	
	if(result != Z_OK) {
		const int msg_len = 64;
		char exception_msg[msg_len];
		snprintf(exception_msg, msg_len, "Zlib-Decompressor returned error: %d", result);
		THROW(env, "java/lang/InternalError", exception_msg);
     	}
     	
     	return (jint)number_of_uncompressed_bytes;
}

