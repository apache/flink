JAVAINCLUDES = $(JAVA_HOME)/include

.PHONY: clean zlib

all: zlib
	$(CC) -Wall -fPIC -g -I $(JAVAINCLUDES) -I $(JAVAINCLUDES)/linux -c ZlibCompressor.c
	$(CC) -Wall -fPIC -g -I $(JAVAINCLUDES) -I $(JAVAINCLUDES)/linux -c ZlibDecompressor.c
	$(LD) -shared -soname libzlibcompression.so.1 -o libzlibcompression.so.1.0 ZlibCompressor.o ZlibDecompressor.o adler32.o compress.o crc32.o gzio.o uncompr.o deflate.o trees.o zutil.o inflate.o infback.o inftrees.o inffast.o

zlib:
	$(CC) -O2 -Wall -fPIC -g -c zlib/adler32.c
	$(CC) -O2 -Wall -fPIC -g -c zlib/compress.c
	$(CC) -O2 -Wall -fPIC -g -c zlib/crc32.c
	$(CC) -O2 -Wall -fPIC -g -c zlib/gzio.c
	$(CC) -O2 -Wall -fPIC -g -c zlib/uncompr.c
	$(CC) -O2 -Wall -fPIC -g -c zlib/deflate.c
	$(CC) -O2 -Wall -fPIC -g -c zlib/trees.c
	$(CC) -O2 -Wall -fPIC -g -c zlib/zutil.c
	$(CC) -O2 -Wall -fPIC -g -c zlib/inflate.c
	$(CC) -O2 -Wall -fPIC -g -c zlib/infback.c
	$(CC) -O2 -Wall -fPIC -g -c zlib/inftrees.c
	$(CC) -O2 -Wall -fPIC -g -c zlib/inffast.c


clean:
	rm -rf *.so
	rm -rf *.o
	rm -rf *.so.*
