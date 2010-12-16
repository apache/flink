#include "LzoCompressor.h"
#include "lzo/minilzo.h"

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

lzo_bytep src;
lzo_bytep dest;

#define HEAP_ALLOC(var,size) \
    lzo_align_t __LZO_MMODEL var [ ((size) + (sizeof(lzo_align_t) - 1)) / sizeof(lzo_align_t) ]

static HEAP_ALLOC(wrkmem,LZO1X_1_MEM_COMPRESS);
  
static jfieldID LzoCompressor_uncompressedDataBuffer;
static jfieldID LzoCompressor_uncompressedDataBufferLength;
static jfieldID LzoCompressor_compressedDataBuffer;
static jfieldID LzoCompressor_compressedDataBufferLength;

JNIEXPORT void JNICALL Java_de_tu_1berlin_cit_nephele_io_compression_library_lzo_LzoCompressor_initIDs (JNIEnv *env, jclass class){

	LzoCompressor_uncompressedDataBuffer = (*env)->GetFieldID(env, class, "uncompressedDataBuffer", "Ljava/nio/ByteBuffer;");
	LzoCompressor_uncompressedDataBufferLength = (*env)->GetFieldID(env, class, "uncompressedDataBufferLength", "I");
	LzoCompressor_compressedDataBuffer = (*env)->GetFieldID(env, class, "compressedDataBuffer", "Ljava/nio/ByteBuffer;");
	LzoCompressor_compressedDataBufferLength = (*env)->GetFieldID(env, class, "compressedDataBufferLength", "I");
	
}

JNIEXPORT void JNICALL Java_de_tu_1berlin_cit_nephele_io_compression_library_lzo_LzoCompressor_init (JNIEnv *env, jobject this){
    if (lzo_init() != LZO_E_OK){
        	const int msg_len = 64;
		      char exception_msg[msg_len];
		      snprintf(exception_msg, msg_len, "Lzo-Compressor failed during initialization!");
		      THROW(env, "java/lang/InternalError", exception_msg);
    }
}

JNIEXPORT jint JNICALL Java_de_tu_1berlin_cit_nephele_io_compression_library_lzo_LzoCompressor_compressBytesDirect (JNIEnv *env, jobject this, jint offset){
        
            // Get buffers of LzoCompressor  
        jobject uncompressed_buf = (*env)->GetObjectField(env, this, LzoCompressor_uncompressedDataBuffer);
        jobject compressed_buf = (*env)->GetObjectField(env, this, LzoCompressor_compressedDataBuffer);
    
        dest = (*env)->GetDirectBufferAddress(env, compressed_buf);
        src = (*env)->GetDirectBufferAddress(env, uncompressed_buf);
        
       if (dest == NULL || src == NULL || wrkmem == NULL)
  		      return (jint)0;
  		      
  		  dest += offset;
       
        // Get members of LzoCompressor
        lzo_uint uncompressed_buf_len = (*env)->GetIntField(env, this, LzoCompressor_uncompressedDataBufferLength);
	      lzo_uint compressed_buf_len = (*env)->GetIntField(env, this, LzoCompressor_compressedDataBufferLength);
	      
	      lzo_uint in_len = uncompressed_buf_len;
	      lzo_uint out_len = compressed_buf_len;
	      int compResult = 0;
	      
        //compress
        compResult = lzo1x_1_compress(src, in_len, dest + SIZE_LENGTH + UNCOMPRESSED_BLOCKSIZE_LENGTH, &out_len, wrkmem);
        
	      
	      //write length of compressed size to compressed buffer
        *(dest) = (unsigned char) ((out_len >> 24) & 0xff);
	      *(dest + 1) = (unsigned char) ((out_len >> 16) & 0xff);
	      *(dest + 2) = (unsigned char) ((out_len >> 8) & 0xff);
	      *(dest + 3) = (unsigned char) ((out_len >> 0) & 0xff);

	      //write length of uncompressed size to compressed buffer
        *(dest + 4) = (unsigned char) ((in_len >> 24) & 0xff);
	      *(dest + 5) = (unsigned char) ((in_len >> 16) & 0xff);
	      *(dest + 6) = (unsigned char) ((in_len >> 8) & 0xff);
	      *(dest + 7) = (unsigned char) ((in_len >> 0) & 0xff);
           	      
	      //lzo_free(wrkmem);
	      
	     if (compResult != LZO_E_OK || out_len >in_len + in_len / 16 + 64 +3) {
          const int msg_len = 64;
		      char exception_msg[msg_len];
		      snprintf(exception_msg, msg_len, "Lzo-Compressor returned error: %d", compResult);
		      THROW(env, "java/lang/InternalError", exception_msg);

	     } else {
          // lzo compresses all input data
		      (*env)->SetIntField(env, this, LzoCompressor_uncompressedDataBufferLength, 0);
     	}
     	
     	return (jint)out_len;
}
