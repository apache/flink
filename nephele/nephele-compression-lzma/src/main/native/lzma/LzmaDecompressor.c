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
#include "LzmaDecompressor.h"
#include "lzma/LzmaLib.h"
#include "lzma/LzmaDec.h"
#include "lzma/Types.h"
#include "lzma/Alloc.h"
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

#define SIZE_LENGTH 4
#define MY_STDAPI int MY_STD_CALL
#define LZMA_PROPS_SIZE 5

static jfieldID LzmaDecompressor_compressedDataBuffer;
static jfieldID LzmaDecompressor_compressedDataBufferLength;
static jfieldID LzmaDecompressor_uncompressedDataBuffer;
static jfieldID LzmaDecompressor_uncompressedDataBufferLength;

JNIEXPORT void JNICALL Java_de_tu_1berlin_cit_nephele_io_compression_library_lzma_LzmaDecompressor_initIDs (JNIEnv *env, jclass class){

	LzmaDecompressor_compressedDataBuffer = (*env)->GetFieldID(env, class, "compressedDataBuffer", "Ljava/nio/ByteBuffer;");
	LzmaDecompressor_compressedDataBufferLength = (*env)->GetFieldID(env, class, "compressedDataBufferLength", "I");
	LzmaDecompressor_uncompressedDataBuffer = (*env)->GetFieldID(env, class, "uncompressedDataBuffer", "Ljava/nio/ByteBuffer;");
	LzmaDecompressor_uncompressedDataBufferLength = (*env)->GetFieldID(env, class, "uncompressedDataBufferLength", "I");
	
}

JNIEXPORT jint JNICALL Java_de_tu_1berlin_cit_nephele_io_compression_library_lzma_LzmaDecompressor_decompressBytesDirect (JNIEnv *env, jobject this, 
                                                                                                                      jint offset){
        
        // Get members of Bzip2Decompressor  
        jobject uncompressed_buf = (*env)->GetObjectField(env, this, LzmaDecompressor_uncompressedDataBuffer);
        jobject compressed_buf = (*env)->GetObjectField(env, this, LzmaDecompressor_compressedDataBuffer);
        
        unsigned char *src = (*env)->GetDirectBufferAddress(env, compressed_buf);
        unsigned char *dest = (*env)->GetDirectBufferAddress(env, uncompressed_buf);
        
        jint uncompressed_buf_len = (*env)->GetIntField(env, this, LzmaDecompressor_uncompressedDataBufferLength);
  	jint compressed_buf_len = (*env)->GetIntField(env, this, LzmaDecompressor_compressedDataBufferLength);  
        
	size_t dest_len = uncompressed_buf_len;
	size_t src_len = compressed_buf_len;
      	
	unsigned char *outProps = src + offset ;
   
 	SRes decompResult = LzmaUncompress(dest, &dest_len, src+offset+LZMA_PROPS_SIZE, &src_len, outProps, LZMA_PROPS_SIZE);
        if (decompResult != SZ_OK) {
		      const int msg_len = 128;
		      char exception_msg[msg_len];
		      snprintf(exception_msg, msg_len, "LZMA-Decomp returned: %d", decompResult);
		      THROW(env, "java/lang/InternalError", exception_msg);
	      }
	      
	      return (jint)dest_len;
}


