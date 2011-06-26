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

#include "SnappyDecompressor.h"
#include "snappy/snappy-c.h"
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
static jfieldID SnappyDecompressor_uncompressedDataBuffer;
static jfieldID SnappyDecompressor_uncompressedDataBufferLength;
static jfieldID SnappyDecompressor_compressedDataBuffer;
static jfieldID SnappyDecompressor_compressedDataBufferLength;

JNIEXPORT void JNICALL Java_de_tu_1berlin_cit_nephele_io_compression_library_snappy_SnappyDecompressor_initIDs (JNIEnv *env, jclass class){

	SnappyDecompressor_uncompressedDataBuffer = (*env)->GetFieldID(env, class, "uncompressedDataBuffer", "Ljava/nio/ByteBuffer;");
	SnappyDecompressor_uncompressedDataBufferLength = (*env)->GetFieldID(env, class, "uncompressedDataBufferLength", "I");
	SnappyDecompressor_compressedDataBuffer = (*env)->GetFieldID(env, class, "compressedDataBuffer", "Ljava/nio/ByteBuffer;");
	SnappyDecompressor_compressedDataBufferLength = (*env)->GetFieldID(env, class, "compressedDataBufferLength", "I");
	
}

JNIEXPORT jint JNICALL Java_de_tu_1berlin_cit_nephele_io_compression_library_snappy_SnappyDecompressor_decompressBytesDirect (JNIEnv *env, jobject this, jint offset){
                                                                                                                    
        // Get members of ZlibDecompressor  
        jobject uncompressed_buf = (*env)->GetObjectField(env, this, SnappyDecompressor_uncompressedDataBuffer);
        jobject compressed_buf = (*env)->GetObjectField(env, this, SnappyDecompressor_compressedDataBuffer);
        
	char *src = (*env)->GetDirectBufferAddress(env, compressed_buf);
        char *dest = (*env)->GetDirectBufferAddress(env, uncompressed_buf);
	
	jint compressed_buf_len = (*env)->GetIntField(env, this, SnappyDecompressor_compressedDataBufferLength);
	jint uncompressed_buf_len = (*env)->GetIntField(env, this, SnappyDecompressor_uncompressedDataBufferLength);

	size_t number_of_uncompressed_bytes = uncompressed_buf_len;

	snappy_status status = snappy_uncompress(src+offset, (size_t) compressed_buf_len-offset, dest, &number_of_uncompressed_bytes);
	
	if(status != SNAPPY_OK) {
		const int msg_len = 64;
		char exception_msg[msg_len];
		snprintf(exception_msg, msg_len, "Snappy-Decompressor returned error: %d", status);
		THROW(env, "java/lang/InternalError", exception_msg);
     	}
     	
     	return (jint)number_of_uncompressed_bytes;
}

