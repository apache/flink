/* XzCrc64.c -- CRC64 calculation
2009-04-15 : Igor Pavlov : Public domain */

#ifndef __XZ_CRC64_H
#define __XZ_CRC64_H

#include <stddef.h>

#include "Types.h"

#ifdef __cplusplus
extern "C" {
#endif

extern UInt64 g_Crc64Table[];

void MY_FAST_CALL Crc64GenerateTable(void);

#define CRC64_INIT_VAL 0xFFFFFFFFFFFFFFFF
#define CRC64_GET_DIGEST(crc) ((crc) ^ 0xFFFFFFFFFFFFFFFF)
#define CRC64_UPDATE_BYTE(crc, b) (g_Crc64Table[((crc) ^ (b)) & 0xFF] ^ ((crc) >> 8))

UInt64 MY_FAST_CALL Crc64Update(UInt64 crc, const void *data, size_t size);
UInt64 MY_FAST_CALL Crc64Calc(const void *data, size_t size);

#ifdef __cplusplus
}
#endif

#endif
