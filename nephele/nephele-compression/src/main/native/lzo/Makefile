JAVAINCLUDES = $(JAVA_HOME)/include

.PHONY: clean lzo

all: lzo 

	$(CC) -Wall -fPIC -g -I $(JAVAINCLUDES) -I $(JAVAINCLUDES)/linux -c LzoCompressor.c
	$(CC) -Wall -fPIC -g -I $(JAVAINCLUDES) -I $(JAVAINCLUDES)/linux -c LzoDecompressor.c
	$(LD) -shared -soname liblzocompression.so.1 -o liblzocompression.so.1.0 LzoCompressor.o LzoDecompressor.o minilzo.o

lzo:
	$(CC) -s -O2 -Wall -fomit-frame-pointer -fPIC -g -c lzo/minilzo.c

clean:
	rm -rf *.o
	rm -rf *.so
	rm -rf *.so.*
	
