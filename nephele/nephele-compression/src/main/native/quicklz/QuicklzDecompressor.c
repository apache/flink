#include "QuicklzDecompressor.h"
#include "quicklz.h"
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

static jfieldID QuicklzDecompressor_uncompressedDataBuffer;
static jfieldID QuicklzDecompressor_uncompressedDataBufferLength;
static jfieldID QuicklzDecompressor_compressedDataBuffer;
static jfieldID QuicklzDecompressor_compressedDataBufferLength;
char *scratch;

JNIEXPORT void JNICALL Java_de_tu_1berlin_cit_nephele_io_compression_library_quicklz_QuicklzDecompressor_initIDs (JNIEnv *env, jclass class){

	scratch = (char *)malloc(QLZ_SCRATCH_COMPRESS);

	QuicklzDecompressor_uncompressedDataBuffer = (*env)->GetFieldID(env, class, "uncompressedDataBuffer", "Ljava/nio/ByteBuffer;");
	QuicklzDecompressor_uncompressedDataBufferLength = (*env)->GetFieldID(env, class, "uncompressedDataBufferLength", "I");
	QuicklzDecompressor_compressedDataBuffer = (*env)->GetFieldID(env, class, "compressedDataBuffer", "Ljava/nio/ByteBuffer;");
	QuicklzDecompressor_compressedDataBufferLength = (*env)->GetFieldID(env, class, "compressedDataBufferLength", "I");
	
}

JNIEXPORT jint JNICALL Java_de_tu_1berlin_cit_nephele_io_compression_library_quicklz_QuicklzDecompressor_decompressBytesDirect (JNIEnv *env, jobject this, jint offset){
                                                                                                                    
        // Get members of QuicklzDecompressor  
        jobject uncompressed_buf = (*env)->GetObjectField(env, this, QuicklzDecompressor_uncompressedDataBuffer);
        jobject compressed_buf = (*env)->GetObjectField(env, this, QuicklzDecompressor_compressedDataBuffer);
        
	const char *src = (*env)->GetDirectBufferAddress(env, compressed_buf);
        char *dest = (*env)->GetDirectBufferAddress(env, uncompressed_buf);
	
	//jint compressed_buf_len = (*env)->GetIntField(env, this, QuicklzDecompressor_compressedDataBufferLength);
	//jint uncompressed_buf_len = (*env)->GetIntField(env, this, QuicklzDecompressor_uncompressedDataBufferLength);

	int result = qlz_decompress(src + offset, dest, scratch);
	
	//TODO: check for correct error codes  
	if(result < 0) {
		const int msg_len = 64;
		char exception_msg[msg_len];
		snprintf(exception_msg, msg_len, "Quicklz-Decompressor returned error: %d", result);
		THROW(env, "java/lang/InternalError", exception_msg);
     	}
     	
     	return (jint)result;
}

