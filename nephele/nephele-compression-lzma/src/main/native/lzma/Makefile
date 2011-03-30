JAVAINCLUDES = $(JAVA_HOME)/include

.PHONY: clean lzma

all: lzma

	$(CC) -Wall -fPIC -g -I $(JAVAINCLUDES) -I $(JAVAINCLUDES)/linux -c LzmaCompressor.c
	$(CC) -Wall -fPIC -g -I $(JAVAINCLUDES) -I $(JAVAINCLUDES)/linux -c LzmaDecompressor.c
	$(LD) -shared -soname liblzmacompression.so.1 -o liblzmacompression.so.1.0 LzmaCompressor.o LzmaDecompressor.o LzmaDec.o LzmaEnc.o LzFind.o LzmaLib.o Alloc.o

lzma:
	$(CC) -O2 -Wall -fPIC -g -c lzma/LzmaDec.c
	$(CC) -O2 -Wall -fPIC -g -c lzma/LzmaEnc.c
	$(CC) -O2 -Wall -fPIC -g -c lzma/LzFind.c
	#g++ -O2 -Wall -fPIC -g -I /opt/jdk1.6.0_18/include -I /opt/jdk1.6.0_18/include/linux -c -D_7ZIP_ST lzma/LzFindMt.c 
	$(CC) -O2 -Wall -fPIC -g -c lzma/LzmaLib.c
	$(CC) -O2 -Wall -fPIC -g -c lzma/Alloc.c

clean:
	rm -rf *.so
	rm -rf *.o
	rm -rf *.so.*

