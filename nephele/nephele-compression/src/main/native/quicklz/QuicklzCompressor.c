#include <stdlib.h>
#include "QuicklzCompressor.h"
#include "quicklz.h"

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

static jfieldID QuicklzCompressor_uncompressedDataBuffer;
static jfieldID QuicklzCompressor_uncompressedDataBufferLength;
static jfieldID QuicklzCompressor_compressedDataBuffer;
static jfieldID QuicklzCompressor_compressedDataBufferLength;
char *scratch;

JNIEXPORT void JNICALL Java_de_tu_1berlin_cit_nephele_io_compression_library_quicklz_QuicklzCompressor_initIDs (JNIEnv *env, jclass class){

	scratch = (char *)malloc(QLZ_SCRATCH_COMPRESS);

	QuicklzCompressor_uncompressedDataBuffer = (*env)->GetFieldID(env, class, "uncompressedDataBuffer", "Ljava/nio/ByteBuffer;");
	QuicklzCompressor_uncompressedDataBufferLength = (*env)->GetFieldID(env, class, "uncompressedDataBufferLength", "I");
	QuicklzCompressor_compressedDataBuffer = (*env)->GetFieldID(env, class, "compressedDataBuffer", "Ljava/nio/ByteBuffer;");
	QuicklzCompressor_compressedDataBufferLength = (*env)->GetFieldID(env, class, "compressedDataBufferLength", "I");
	
}

JNIEXPORT jint JNICALL Java_de_tu_1berlin_cit_nephele_io_compression_library_quicklz_QuicklzCompressor_compressBytesDirect (JNIEnv *env, jobject this, jint offset){
                     
        // Get members of QuicklzCompressor
        jobject uncompressed_buf = (*env)->GetObjectField(env, this, QuicklzCompressor_uncompressedDataBuffer);
        jint uncompressed_buf_len = (*env)->GetIntField(env, this, QuicklzCompressor_uncompressedDataBufferLength);

        jobject compressed_buf = (*env)->GetObjectField(env, this, QuicklzCompressor_compressedDataBuffer);
	//jint compressed_buf_len = (*env)->GetIntField(env, this, QuicklzCompressor_compressedDataBufferLength);
	      
	// Get access to the uncompressed buffer object
	const char *src = (*env)->GetDirectBufferAddress(env, uncompressed_buf);

	if (src == 0){
		const int msg_len = 64;
		char exception_msg[msg_len];
		snprintf(exception_msg, msg_len, "Quicklz-Compressor - No access to uncompressed-buffer");
		THROW(env, "java/lang/InternalError", exception_msg);
		return (jint)0;
        }


        // Get access to the compressed buffer object
        char *dest = (*env)->GetDirectBufferAddress(env, compressed_buf);

      	if (dest == 0){
	      const int msg_len = 64;
	      char exception_msg[msg_len];
	      snprintf(exception_msg, msg_len, "Quicklz-Compressor - no access to compressed-buffer");
	      THROW(env, "java/lang/InternalError", exception_msg);
	      return (jint)0;
		      
        }
              	
        dest += offset;
              	
        //size_t no_compressed_bytes = compressed_buf_len;
        size_t no_uncompressed_bytes = uncompressed_buf_len;
        
        int result = qlz_compress(src, dest + SIZE_LENGTH, no_uncompressed_bytes, scratch);
                              
        //write length of compressed size to compressed buffer
        *(dest) = (char) ((result >> 24) & 0xff);
	      *(dest + 1) = (char) ((result >> 16) & 0xff);
	      *(dest + 2) = (char) ((result >> 8) & 0xff);
	      *(dest + 3) = (char) ((result >> 0) & 0xff);

	      //write length of uncompressed size to compressed buffer
        *(dest + 4) = (char) ((no_uncompressed_bytes >> 24) & 0xff);
	      *(dest + 5) = (char) ((no_uncompressed_bytes >> 16) & 0xff);
	      *(dest + 6) = (char) ((no_uncompressed_bytes >> 8) & 0xff);
	      *(dest + 7) = (char) ((no_uncompressed_bytes >> 0) & 0xff);
                 
	//TODO: check for correct error codes                            
        if (result < 0) {
		const int msg_len = 32;
		char exception_msg[msg_len];
		snprintf(exception_msg, msg_len, "Quciklz-Compressor returned: %d", result);
		THROW(env, "java/lang/InternalError", exception_msg);
     	}else{
		(*env)->SetIntField(env, this, QuicklzCompressor_uncompressedDataBufferLength, 0);
	}
     	
     	return (jint)result;

}
