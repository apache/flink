/* lzoconf.h -- configuration for the LZO real-time data compression library

   This file is part of the LZO real-time data compression library.

   Copyright (C) 2008 Markus Franz Xaver Johannes Oberhumer
   Copyright (C) 2007 Markus Franz Xaver Johannes Oberhumer
   Copyright (C) 2006 Markus Franz Xaver Johannes Oberhumer
   Copyright (C) 2005 Markus Franz Xaver Johannes Oberhumer
   Copyright (C) 2004 Markus Franz Xaver Johannes Oberhumer
   Copyright (C) 2003 Markus Franz Xaver Johannes Oberhumer
   Copyright (C) 2002 Markus Franz Xaver Johannes Oberhumer
   Copyright (C) 2001 Markus Franz Xaver Johannes Oberhumer
   Copyright (C) 2000 Markus Franz Xaver Johannes Oberhumer
   Copyright (C) 1999 Markus Franz Xaver Johannes Oberhumer
   Copyright (C) 1998 Markus Franz Xaver Johannes Oberhumer
   Copyright (C) 1997 Markus Franz Xaver Johannes Oberhumer
   Copyright (C) 1996 Markus Franz Xaver Johannes Oberhumer
   All Rights Reserved.

   The LZO library is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License as
   published by the Free Software Foundation; either version 2 of
   the License, or (at your option) any later version.

   The LZO library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with the LZO library; see the file COPYING.
   If not, write to the Free Software Foundation, Inc.,
   51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

   Markus F.X.J. Oberhumer
   <markus@oberhumer.com>
   http://www.oberhumer.com/opensource/lzo/
 */


#ifndef __LZOCONF_H_INCLUDED
#define __LZOCONF_H_INCLUDED

#define LZO_VERSION             0x2030
#define LZO_VERSION_STRING      "2.03"
#define LZO_VERSION_DATE        "Apr 30 2008"

/* internal Autoconf configuration file - only used when building LZO */
#if defined(LZO_HAVE_CONFIG_H)
#  include <config.h>
#endif
#include <limits.h>
#include <stddef.h>


/***********************************************************************
// LZO requires a conforming <limits.h>
************************************************************************/

#if !defined(CHAR_BIT) || (CHAR_BIT != 8)
#  error "invalid CHAR_BIT"
#endif
#if !defined(UCHAR_MAX) || !defined(UINT_MAX) || !defined(ULONG_MAX)
#  error "check your compiler installation"
#endif
#if (USHRT_MAX < 1) || (UINT_MAX < 1) || (ULONG_MAX < 1)
#  error "your limits.h macros are broken"
#endif

/* get OS and architecture defines */
#ifndef __LZODEFS_H_INCLUDED
#include "lzodefs.h"
#endif


#ifdef __cplusplus
extern "C" {
#endif


/***********************************************************************
// some core defines
************************************************************************/

#if !defined(LZO_UINT32_C)
#  if (UINT_MAX < LZO_0xffffffffL)
#    define LZO_UINT32_C(c)     c ## UL
#  else
#    define LZO_UINT32_C(c)     ((c) + 0U)
#  endif
#endif

/* memory checkers */
#if !defined(__LZO_CHECKER)
#  if defined(__BOUNDS_CHECKING_ON)
#    define __LZO_CHECKER       1
#  elif defined(__CHECKER__)
#    define __LZO_CHECKER       1
#  elif defined(__INSURE__)
#    define __LZO_CHECKER       1
#  elif defined(__PURIFY__)
#    define __LZO_CHECKER       1
#  endif
#endif


/***********************************************************************
// integral and pointer types
************************************************************************/

/* lzo_uint should match size_t */
#if !defined(LZO_UINT_MAX)
#  if defined(LZO_ABI_LLP64) /* WIN64 */
#    if defined(LZO_OS_WIN64)
     typedef unsigned __int64   lzo_uint;
     typedef __int64            lzo_int;
#    else
     typedef unsigned long long lzo_uint;
     typedef long long          lzo_int;
#    endif
#    define LZO_UINT_MAX        0xffffffffffffffffull
#    define LZO_INT_MAX         9223372036854775807LL
#    define LZO_INT_MIN         (-1LL - LZO_INT_MAX)
#  elif defined(LZO_ABI_IP32L64) /* MIPS R5900 */
     typedef unsigned int       lzo_uint;
     typedef int                lzo_int;
#    define LZO_UINT_MAX        UINT_MAX
#    define LZO_INT_MAX         INT_MAX
#    define LZO_INT_MIN         INT_MIN
#  elif (ULONG_MAX >= LZO_0xffffffffL)
     typedef unsigned long      lzo_uint;
     typedef long               lzo_int;
#    define LZO_UINT_MAX        ULONG_MAX
#    define LZO_INT_MAX         LONG_MAX
#    define LZO_INT_MIN         LONG_MIN
#  else
#    error "lzo_uint"
#  endif
#endif

/* Integral types with 32 bits or more. */
#if !defined(LZO_UINT32_MAX)
#  if (UINT_MAX >= LZO_0xffffffffL)
     typedef unsigned int       lzo_uint32;
     typedef int                lzo_int32;
#    define LZO_UINT32_MAX      UINT_MAX
#    define LZO_INT32_MAX       INT_MAX
#    define LZO_INT32_MIN       INT_MIN
#  elif (ULONG_MAX >= LZO_0xffffffffL)
     typedef unsigned long      lzo_uint32;
     typedef long               lzo_int32;
#    define LZO_UINT32_MAX      ULONG_MAX
#    define LZO_INT32_MAX       LONG_MAX
#    define LZO_INT32_MIN       LONG_MIN
#  else
#    error "lzo_uint32"
#  endif
#endif

/* The larger type of lzo_uint and lzo_uint32. */
#if (LZO_UINT_MAX >= LZO_UINT32_MAX)
#  define lzo_xint              lzo_uint
#else
#  define lzo_xint              lzo_uint32
#endif

/* Memory model that allows to access memory at offsets of lzo_uint. */
#if !defined(__LZO_MMODEL)
#  if (LZO_UINT_MAX <= UINT_MAX)
#    define __LZO_MMODEL
#  elif defined(LZO_HAVE_MM_HUGE_PTR)
#    define __LZO_MMODEL_HUGE   1
#    define __LZO_MMODEL        __huge
#  else
#    define __LZO_MMODEL
#  endif
#endif

/* no typedef here because of const-pointer issues */
#define lzo_bytep               unsigned char __LZO_MMODEL *
#define lzo_charp               char __LZO_MMODEL *
#define lzo_voidp               void __LZO_MMODEL *
#define lzo_shortp              short __LZO_MMODEL *
#define lzo_ushortp             unsigned short __LZO_MMODEL *
#define lzo_uint32p             lzo_uint32 __LZO_MMODEL *
#define lzo_int32p              lzo_int32 __LZO_MMODEL *
#define lzo_uintp               lzo_uint __LZO_MMODEL *
#define lzo_intp                lzo_int __LZO_MMODEL *
#define lzo_xintp               lzo_xint __LZO_MMODEL *
#define lzo_voidpp              lzo_voidp __LZO_MMODEL *
#define lzo_bytepp              lzo_bytep __LZO_MMODEL *
/* deprecated - use `lzo_bytep' instead of `lzo_byte *' */
#define lzo_byte                unsigned char __LZO_MMODEL

typedef int lzo_bool;


/***********************************************************************
// function types
************************************************************************/

/* name mangling */
#if !defined(__LZO_EXTERN_C)
#  ifdef __cplusplus
#    define __LZO_EXTERN_C      extern "C"
#  else
#    define __LZO_EXTERN_C      extern
#  endif
#endif

/* calling convention */
#if !defined(__LZO_CDECL)
#  define __LZO_CDECL           __lzo_cdecl
#endif

/* DLL export information */
#if !defined(__LZO_EXPORT1)
#  define __LZO_EXPORT1
#endif
#if !defined(__LZO_EXPORT2)
#  define __LZO_EXPORT2
#endif

/* __cdecl calling convention for public C and assembly functions */
#if !defined(LZO_PUBLIC)
#  define LZO_PUBLIC(_rettype)  __LZO_EXPORT1 _rettype __LZO_EXPORT2 __LZO_CDECL
#endif
#if !defined(LZO_EXTERN)
#  define LZO_EXTERN(_rettype)  __LZO_EXTERN_C LZO_PUBLIC(_rettype)
#endif
#if !defined(LZO_PRIVATE)
#  define LZO_PRIVATE(_rettype) static _rettype __LZO_CDECL
#endif

/* function types */
typedef int
(__LZO_CDECL *lzo_compress_t)   ( const lzo_bytep src, lzo_uint  src_len,
                                        lzo_bytep dst, lzo_uintp dst_len,
                                        lzo_voidp wrkmem );

typedef int
(__LZO_CDECL *lzo_decompress_t) ( const lzo_bytep src, lzo_uint  src_len,
                                        lzo_bytep dst, lzo_uintp dst_len,
                                        lzo_voidp wrkmem );

typedef int
(__LZO_CDECL *lzo_optimize_t)   (       lzo_bytep src, lzo_uint  src_len,
                                        lzo_bytep dst, lzo_uintp dst_len,
                                        lzo_voidp wrkmem );

typedef int
(__LZO_CDECL *lzo_compress_dict_t)(const lzo_bytep src, lzo_uint  src_len,
                                         lzo_bytep dst, lzo_uintp dst_len,
                                         lzo_voidp wrkmem,
                                   const lzo_bytep dict, lzo_uint dict_len );

typedef int
(__LZO_CDECL *lzo_decompress_dict_t)(const lzo_bytep src, lzo_uint  src_len,
                                           lzo_bytep dst, lzo_uintp dst_len,
                                           lzo_voidp wrkmem,
                                     const lzo_bytep dict, lzo_uint dict_len );


/* Callback interface. Currently only the progress indicator ("nprogress")
 * is used, but this may change in a future release. */

struct lzo_callback_t;
typedef struct lzo_callback_t lzo_callback_t;
#define lzo_callback_p lzo_callback_t __LZO_MMODEL *

/* malloc & free function types */
typedef lzo_voidp (__LZO_CDECL *lzo_alloc_func_t)
    (lzo_callback_p self, lzo_uint items, lzo_uint size);
typedef void      (__LZO_CDECL *lzo_free_func_t)
    (lzo_callback_p self, lzo_voidp ptr);

/* a progress indicator callback function */
typedef void (__LZO_CDECL *lzo_progress_func_t)
    (lzo_callback_p, lzo_uint, lzo_uint, int);

struct lzo_callback_t
{
    /* custom allocators (set to 0 to disable) */
    lzo_alloc_func_t nalloc;                /* [not used right now] */
    lzo_free_func_t nfree;                  /* [not used right now] */

    /* a progress indicator callback function (set to 0 to disable) */
    lzo_progress_func_t nprogress;

    /* NOTE: the first parameter "self" of the nalloc/nfree/nprogress
     * callbacks points back to this struct, so you are free to store
     * some extra info in the following variables. */
    lzo_voidp user1;
    lzo_xint user2;
    lzo_xint user3;
};


/***********************************************************************
// error codes and prototypes
************************************************************************/

/* Error codes for the compression/decompression functions. Negative
 * values are errors, positive values will be used for special but
 * normal events.
 */
#define LZO_E_OK                    0
#define LZO_E_ERROR                 (-1)
#define LZO_E_OUT_OF_MEMORY         (-2)    /* [not used right now] */
#define LZO_E_NOT_COMPRESSIBLE      (-3)    /* [not used right now] */
#define LZO_E_INPUT_OVERRUN         (-4)
#define LZO_E_OUTPUT_OVERRUN        (-5)
#define LZO_E_LOOKBEHIND_OVERRUN    (-6)
#define LZO_E_EOF_NOT_FOUND         (-7)
#define LZO_E_INPUT_NOT_CONSUMED    (-8)
#define LZO_E_NOT_YET_IMPLEMENTED   (-9)    /* [not used right now] */


#ifndef lzo_sizeof_dict_t
#  define lzo_sizeof_dict_t     ((unsigned)sizeof(lzo_bytep))
#endif

/* lzo_init() should be the first function you call.
 * Check the return code !
 *
 * lzo_init() is a macro to allow checking that the library and the
 * compiler's view of various types are consistent.
 */
#define lzo_init() __lzo_init_v2(LZO_VERSION,(int)sizeof(short),(int)sizeof(int),\
    (int)sizeof(long),(int)sizeof(lzo_uint32),(int)sizeof(lzo_uint),\
    (int)lzo_sizeof_dict_t,(int)sizeof(char *),(int)sizeof(lzo_voidp),\
    (int)sizeof(lzo_callback_t))
LZO_EXTERN(int) __lzo_init_v2(unsigned,int,int,int,int,int,int,int,int,int);

/* version functions (useful for shared libraries) */
LZO_EXTERN(unsigned) lzo_version(void);
LZO_EXTERN(const char *) lzo_version_string(void);
LZO_EXTERN(const char *) lzo_version_date(void);
LZO_EXTERN(const lzo_charp) _lzo_version_string(void);
LZO_EXTERN(const lzo_charp) _lzo_version_date(void);

/* string functions */
LZO_EXTERN(int)
lzo_memcmp(const lzo_voidp _s1, const lzo_voidp _s2, lzo_uint _len);
LZO_EXTERN(lzo_voidp)
lzo_memcpy(lzo_voidp _dest, const lzo_voidp _src, lzo_uint _len);
LZO_EXTERN(lzo_voidp)
lzo_memmove(lzo_voidp _dest, const lzo_voidp _src, lzo_uint _len);
LZO_EXTERN(lzo_voidp)
lzo_memset(lzo_voidp _s, int _c, lzo_uint _len);

/* checksum functions */
LZO_EXTERN(lzo_uint32)
lzo_adler32(lzo_uint32 _adler, const lzo_bytep _buf, lzo_uint _len);
LZO_EXTERN(lzo_uint32)
lzo_crc32(lzo_uint32 _c, const lzo_bytep _buf, lzo_uint _len);
LZO_EXTERN(const lzo_uint32p)
lzo_get_crc32_table(void);

/* misc. */
LZO_EXTERN(int) _lzo_config_check(void);
typedef union { lzo_bytep p; lzo_uint u; } __lzo_pu_u;
typedef union { lzo_bytep p; lzo_uint32 u32; } __lzo_pu32_u;
typedef union { void *vp; lzo_bytep bp; lzo_uint32 u32; long l; } lzo_align_t;

/* align a char pointer on a boundary that is a multiple of `size' */
LZO_EXTERN(unsigned) __lzo_align_gap(const lzo_voidp _ptr, lzo_uint _size);
#define LZO_PTR_ALIGN_UP(_ptr,_size) \
    ((_ptr) + (lzo_uint) __lzo_align_gap((const lzo_voidp)(_ptr),(lzo_uint)(_size)))


/***********************************************************************
// deprecated macros - only for backward compatibility with LZO v1.xx
************************************************************************/

#if defined(LZO_CFG_COMPAT)

#define __LZOCONF_H 1

#if defined(LZO_ARCH_I086)
#  define __LZO_i386 1
#elif defined(LZO_ARCH_I386)
#  define __LZO_i386 1
#endif

#if defined(LZO_OS_DOS16)
#  define __LZO_DOS 1
#  define __LZO_DOS16 1
#elif defined(LZO_OS_DOS32)
#  define __LZO_DOS 1
#elif defined(LZO_OS_WIN16)
#  define __LZO_WIN 1
#  define __LZO_WIN16 1
#elif defined(LZO_OS_WIN32)
#  define __LZO_WIN 1
#endif

#define __LZO_CMODEL
#define __LZO_DMODEL
#define __LZO_ENTRY             __LZO_CDECL
#define LZO_EXTERN_CDECL        LZO_EXTERN
#define LZO_ALIGN               LZO_PTR_ALIGN_UP

#define lzo_compress_asm_t      lzo_compress_t
#define lzo_decompress_asm_t    lzo_decompress_t

#endif /* LZO_CFG_COMPAT */


#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* already included */


/* vim:set ts=4 et: */
