JAVAINCLUDES = $(JAVA_HOME)/include

.PHONY: clean quicklz

all: quicklz

	$(CC) -Wall -fPIC -g -I $(JAVAINCLUDES) -I $(JAVAINCLUDES)/linux -c QuicklzCompressor.c
	$(CC) -Wall -fPIC -g -I $(JAVAINCLUDES) -I $(JAVAINCLUDES)/linux -c QuicklzDecompressor.c
	$(LD) -shared -soname libquicklzcompression.so.1 -o libquicklzcompression.so.1.0 QuicklzCompressor.o QuicklzDecompressor.o quicklz.o

quicklz:
	$(CC) -O2 -Wall -fPIC -g -c quicklz.c
	#g++ -O2 -Wall -fPIC -g -I /opt/jdk1.6.0_18/include -I /opt/jdk1.6.0_18/include/linux -c -D_7ZIP_ST lzma/LzFindMt.c 


clean:
	rm -rf *.so
	rm -rf *.o
	rm -rf *.so.*

