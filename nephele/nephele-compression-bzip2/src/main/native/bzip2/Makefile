JAVAINCLUDES = $(JAVA_HOME)/include

.PHONY: clean bzip2

all: bzip2

	$(CC) -Wall -fPIC -g -I $(JAVAINCLUDES) -I $(JAVAINCLUDES)/linux -c Bzip2Compressor.c
	$(CC) -Wall -fPIC -g -I $(JAVAINCLUDES) -I $(JAVAINCLUDES)/linux -c Bzip2Decompressor.c
	$(LD) -shared -soname libbzip2compression.so.1 -o libbzip2compression.so.1.0 Bzip2Compressor.o Bzip2Decompressor.o bzlib.o blocksort.o compress.o decompress.o crctable.o huffman.o randtable.o

bzip2:

	$(CC) -O2 -Wall -fPIC -g -c bzip2/bzlib.c
	$(CC) -O2 -Wall -fPIC -g -c bzip2/blocksort.c
	$(CC) -O2 -Wall -fPIC -g -c bzip2/compress.c
	$(CC) -O2 -Wall -fPIC -g -c bzip2/decompress.c
	$(CC) -O2 -Wall -fPIC -g -c bzip2/crctable.c
	$(CC) -O2 -Wall -fPIC -g -c bzip2/huffman.c
	$(CC) -O2 -Wall -fPIC -g -c bzip2/randtable.c

clean:
	rm -rf *.so
	rm -rf *.o
	rm -rf *.so.*

