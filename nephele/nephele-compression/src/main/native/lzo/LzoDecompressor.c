#include "LzoDecompressor.h"
#include "lzo/minilzo.h"
#include <stdlib.h>
#include <string.h>

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
#define UNCOMPRESSED_BLOCKSIZE_LENGTH 4

static jfieldID LzoDecompressor_uncompressedDataBuffer;
static jfieldID LzoDecompressor_uncompressedDataBufferLength;
static jfieldID LzoDecompressor_compressedDataBuffer;
static jfieldID LzoDecompressor_compressedDataBufferLength;

JNIEXPORT void JNICALL Java_de_tu_1berlin_cit_nephele_io_compression_library_lzo_LzoDecompressor_initIDs (JNIEnv *env, jclass class){

	LzoDecompressor_uncompressedDataBuffer = (*env)->GetFieldID(env, class, "uncompressedDataBuffer", "Ljava/nio/ByteBuffer;");
	LzoDecompressor_uncompressedDataBufferLength = (*env)->GetFieldID(env, class, "uncompressedDataBufferLength", "I");
	LzoDecompressor_compressedDataBuffer = (*env)->GetFieldID(env, class, "compressedDataBuffer", "Ljava/nio/ByteBuffer;");
	LzoDecompressor_compressedDataBufferLength = (*env)->GetFieldID(env, class, "compressedDataBufferLength", "I");
	
}

JNIEXPORT void JNICALL Java_de_tu_1berlin_cit_nephele_io_compression_library_lzo_LzoDecompressor_init (JNIEnv *env, jobject this){
    if (lzo_init() != LZO_E_OK){
        	const int msg_len = 64;
		      char exception_msg[msg_len];
		      snprintf(exception_msg, msg_len, "Lzo-Compressor failed during initialization!");
		      THROW(env, "java/lang/InternalError", exception_msg);
    } 
}

JNIEXPORT jint JNICALL Java_de_tu_1berlin_cit_nephele_io_compression_library_lzo_LzoDecompressor_decompressBytesDirect (JNIEnv *env, jobject this, 
                                                                                                                      jint offset){
        int decompResult;
       
        // Get members of LzoDecompressor  
        jobject uncompressed_buf = (*env)->GetObjectField(env, this, LzoDecompressor_uncompressedDataBuffer);
        jobject compressed_buf = (*env)->GetObjectField(env, this, LzoDecompressor_compressedDataBuffer);
        
        unsigned char *src = (*env)->GetDirectBufferAddress(env, compressed_buf);
        unsigned char *dest = (*env)->GetDirectBufferAddress(env, uncompressed_buf);
        
	jint uncompressed_buf_len = (*env)->GetIntField(env, this, LzoDecompressor_uncompressedDataBufferLength);
  	jint compressed_buf_len = (*env)->GetIntField(env, this, LzoDecompressor_compressedDataBufferLength);

	lzo_uint out_len = uncompressed_buf_len;

	//TODO: Change to non-safe version to achieve performance gains              	   
	decompResult = lzo1x_decompress_safe(src+offset, (lzo_uint) (compressed_buf_len-offset), dest, &out_len, NULL );
	if (decompResult != LZO_E_OK) {
        	const int msg_len = 128;
      		char exception_msg[msg_len];
      		snprintf(exception_msg, msg_len, "Lzo-Decompressor returned error: %d %lu %lu", decompResult, (unsigned long)out_len, (unsigned long)compressed_buf_len);
      		        THROW(env, "java/lang/InternalError", exception_msg);
                  return (jint)decompResult;                
	}

	return (jint)out_len; 
}

