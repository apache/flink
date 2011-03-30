#include "Bzip2Decompressor.h"
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

static jfieldID Bzip2Decompressor_uncompressedDataBuffer;
static jfieldID Bzip2Decompressor_uncompressedDataBufferLength;
static jfieldID Bzip2Decompressor_compressedDataBuffer;
static jfieldID Bzip2Decompressor_compressedDataBufferLength;

JNIEXPORT void JNICALL Java_de_tu_1berlin_cit_nephele_io_compression_library_bzip2_Bzip2Decompressor_initIDs (JNIEnv *env, jclass class){

	Bzip2Decompressor_uncompressedDataBuffer = (*env)->GetFieldID(env, class, "uncompressedDataBuffer", "Ljava/nio/ByteBuffer;");
	Bzip2Decompressor_uncompressedDataBufferLength = (*env)->GetFieldID(env, class, "uncompressedDataBufferLength", "I");
	Bzip2Decompressor_compressedDataBuffer = (*env)->GetFieldID(env, class, "compressedDataBuffer", "Ljava/nio/ByteBuffer;");
	Bzip2Decompressor_compressedDataBufferLength = (*env)->GetFieldID(env, class, "compressedDataBufferLength", "I");
	
}

JNIEXPORT jint JNICALL Java_de_tu_1berlin_cit_nephele_io_compression_library_bzip2_Bzip2Decompressor_decompressBytesDirect (JNIEnv *env, jobject this, 
                                                                                                                      jint offset){
        // Get members of Bzip2Decompressor  
        jobject uncompressed_buf = (*env)->GetObjectField(env, this, Bzip2Decompressor_uncompressedDataBuffer);
        jobject compressed_buf = (*env)->GetObjectField(env, this, Bzip2Decompressor_compressedDataBuffer);
    
        char *src = (*env)->GetDirectBufferAddress(env, compressed_buf);
	char *dest = (*env)->GetDirectBufferAddress(env, uncompressed_buf);
       
        jint uncompressed_buf_len = (*env)->GetIntField(env, this, Bzip2Decompressor_uncompressedDataBufferLength);
  	jint compressed_buf_len = (*env)->GetIntField(env, this, Bzip2Decompressor_compressedDataBufferLength);  
        
        //decompress
	unsigned int dest_len = uncompressed_buf_len;
        int decompResult = BZ2_bzBuffToBuffDecompress(dest, &dest_len, src+offset, compressed_buf_len-offset, 0, 0);
        if (decompResult != BZ_OK) {
		const int msg_len = 64;
		char exception_msg[msg_len];
		snprintf(exception_msg, msg_len, "Bzip2-Decompressor returned error: %d", decompResult);
		THROW(env, "java/lang/InternalError", exception_msg);
     	}
     	
     	return (jint)dest_len;
}

