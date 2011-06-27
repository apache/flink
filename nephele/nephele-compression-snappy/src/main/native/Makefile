#Makefile for compression

TARGET_DIR      := ../../../target
JAVA_BUILD_DIR 	= $(TARGET_DIR)/classes
TARGET          = $(JAVA_BUILD_DIR)/META-INF/lib

.PHONY: snappy dir clean

all: dir snappy clean

dir:
	mkdir -p $(TARGET)

snappy:
	make -C snappy
	cp snappy/*.so* $(TARGET)

clean:
	make -C snappy clean
