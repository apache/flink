#Makefile for compression

TARGET_DIR      := ../../../target
JAVA_BUILD_DIR 	= $(TARGET_DIR)/classes
TARGET          = $(JAVA_BUILD_DIR)/META-INF/lib

.PHONY: lzma dir clean

all: dir lzma clean

dir:
	mkdir -p $(TARGET)

lzma:
	make -C lzma
	cp lzma/*.so* $(TARGET)

clean:
	make -C lzma clean
