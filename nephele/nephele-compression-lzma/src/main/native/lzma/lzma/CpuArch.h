/* CpuArch.h
2009-08-11 : Igor Pavlov : Public domain */

#ifndef __CPU_ARCH_H
#define __CPU_ARCH_H

#ifdef __cplusplus
extern "C" {
#endif

/*
LITTLE_ENDIAN_UNALIGN means:
  1) CPU is LITTLE_ENDIAN
  2) it's allowed to make unaligned memory accesses
if LITTLE_ENDIAN_UNALIGN is not defined, it means that we don't know
about these properties of platform.
*/

#if defined(_M_X64) || defined(_M_AMD64) || defined(__x86_64__)
#define MY_CPU_AMD64
#endif

#if defined(MY_CPU_AMD64) || defined(_M_IA64)
#define MY_CPU_64BIT
#endif

#if defined(_M_IX86) || defined(__i386__) || defined(MY_CPU_AMD64)
#define MY_CPU_X86_OR_AMD64
#endif

#if defined(MY_CPU_X86_OR_AMD64)
#define LITTLE_ENDIAN_UNALIGN
#endif

#ifdef LITTLE_ENDIAN_UNALIGN

#define GetUi16(p) (*(const UInt16 *)(p))
#define GetUi32(p) (*(const UInt32 *)(p))
#define GetUi64(p) (*(const UInt64 *)(p))
#define SetUi16(p, d) *(UInt16 *)(p) = (d);
#define SetUi32(p, d) *(UInt32 *)(p) = (d);

#else

#define GetUi16(p) (((const Byte *)(p))[0] | ((UInt16)((const Byte *)(p))[1] << 8))

#define GetUi32(p) ( \
             ((const Byte *)(p))[0]        | \
    ((UInt32)((const Byte *)(p))[1] <<  8) | \
    ((UInt32)((const Byte *)(p))[2] << 16) | \
    ((UInt32)((const Byte *)(p))[3] << 24))

#define GetUi64(p) (GetUi32(p) | ((UInt64)GetUi32(((const Byte *)(p)) + 4) << 32))

#define SetUi16(p, d) { UInt32 _x_ = (d); \
    ((Byte *)(p))[0] = (Byte)_x_; \
    ((Byte *)(p))[1] = (Byte)(_x_ >> 8); }

#define SetUi32(p, d) { UInt32 _x_ = (d); \
    ((Byte *)(p))[0] = (Byte)_x_; \
    ((Byte *)(p))[1] = (Byte)(_x_ >> 8); \
    ((Byte *)(p))[2] = (Byte)(_x_ >> 16); \
    ((Byte *)(p))[3] = (Byte)(_x_ >> 24); }

#endif

#if defined(LITTLE_ENDIAN_UNALIGN) && defined(_WIN64) && (_MSC_VER >= 1300)

#pragma intrinsic(_byteswap_ulong)
#pragma intrinsic(_byteswap_uint64)
#define GetBe32(p) _byteswap_ulong(*(const UInt32 *)(const Byte *)(p))
#define GetBe64(p) _byteswap_uint64(*(const UInt64 *)(const Byte *)(p))

#else

#define GetBe32(p) ( \
    ((UInt32)((const Byte *)(p))[0] << 24) | \
    ((UInt32)((const Byte *)(p))[1] << 16) | \
    ((UInt32)((const Byte *)(p))[2] <<  8) | \
             ((const Byte *)(p))[3] )

#define GetBe64(p) (((UInt64)GetBe32(p) << 32) | GetBe32(((const Byte *)(p)) + 4))

#endif

#define GetBe16(p) (((UInt16)((const Byte *)(p))[0] << 8) | ((const Byte *)(p))[1])

#ifdef __cplusplus
}
#endif

#endif
