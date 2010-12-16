/* minilzo.c -- mini subset of the LZO real-time data compression library

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

/*
 * NOTE:
 *   the full LZO package can be found at
 *   http://www.oberhumer.com/opensource/lzo/
 */

#define __LZO_IN_MINILZO
#define LZO_BUILD

#if defined(LZO_CFG_FREESTANDING)
#  undef MINILZO_HAVE_CONFIG_H
#  define LZO_LIBC_FREESTANDING 1
#  define LZO_OS_FREESTANDING 1
#endif

#ifdef MINILZO_HAVE_CONFIG_H
#  include <config.h>
#endif
#include <limits.h>
#include <stddef.h>
#if defined(MINILZO_CFG_USE_INTERNAL_LZODEFS)

#ifndef __LZODEFS_H_INCLUDED
#define __LZODEFS_H_INCLUDED 1

#if defined(__CYGWIN32__) && !defined(__CYGWIN__)
#  define __CYGWIN__ __CYGWIN32__
#endif
#if defined(__IBMCPP__) && !defined(__IBMC__)
#  define __IBMC__ __IBMCPP__
#endif
#if defined(__ICL) && defined(_WIN32) && !defined(__INTEL_COMPILER)
#  define __INTEL_COMPILER __ICL
#endif
#if 1 && defined(__INTERIX) && defined(__GNUC__) && !defined(_ALL_SOURCE)
#  define _ALL_SOURCE 1
#endif
#if defined(__mips__) && defined(__R5900__)
#  if !defined(__LONG_MAX__)
#    define __LONG_MAX__ 9223372036854775807L
#  endif
#endif
#if defined(__INTEL_COMPILER) && defined(__linux__)
#  pragma warning(disable: 193)
#endif
#if defined(__KEIL__) && defined(__C166__)
#  pragma warning disable = 322
#elif 0 && defined(__C251__)
#  pragma warning disable = 322
#endif
#if defined(_MSC_VER) && !defined(__INTEL_COMPILER) && !defined(__MWERKS__)
#  if (_MSC_VER >= 1300)
#    pragma warning(disable: 4668)
#  endif
#endif
#if 0 && defined(__WATCOMC__)
#  if (__WATCOMC__ >= 1050) && (__WATCOMC__ < 1060)
#    pragma warning 203 9
#  endif
#endif
#if defined(__BORLANDC__) && defined(__MSDOS__) && !defined(__FLAT__)
#  pragma option -h
#endif
#if 0
#define LZO_0xffffL             0xfffful
#define LZO_0xffffffffL         0xfffffffful
#else
#define LZO_0xffffL             65535ul
#define LZO_0xffffffffL         4294967295ul
#endif
#if (LZO_0xffffL == LZO_0xffffffffL)
#  error "your preprocessor is broken 1"
#endif
#if (16ul * 16384ul != 262144ul)
#  error "your preprocessor is broken 2"
#endif
#if 0
#if (32767 >= 4294967295ul)
#  error "your preprocessor is broken 3"
#endif
#if (65535u >= 4294967295ul)
#  error "your preprocessor is broken 4"
#endif
#endif
#if (UINT_MAX == LZO_0xffffL)
#if defined(__ZTC__) && defined(__I86__) && !defined(__OS2__)
#  if !defined(MSDOS)
#    define MSDOS 1
#  endif
#  if !defined(_MSDOS)
#    define _MSDOS 1
#  endif
#elif 0 && defined(__VERSION) && defined(MB_LEN_MAX)
#  if (__VERSION == 520) && (MB_LEN_MAX == 1)
#    if !defined(__AZTEC_C__)
#      define __AZTEC_C__ __VERSION
#    endif
#    if !defined(__DOS__)
#      define __DOS__ 1
#    endif
#  endif
#endif
#endif
#if defined(_MSC_VER) && defined(M_I86HM) && (UINT_MAX == LZO_0xffffL)
#  define ptrdiff_t long
#  define _PTRDIFF_T_DEFINED
#endif
#if (UINT_MAX == LZO_0xffffL)
#  undef __LZO_RENAME_A
#  undef __LZO_RENAME_B
#  if defined(__AZTEC_C__) && defined(__DOS__)
#    define __LZO_RENAME_A 1
#  elif defined(_MSC_VER) && defined(MSDOS)
#    if (_MSC_VER < 600)
#      define __LZO_RENAME_A 1
#    elif (_MSC_VER < 700)
#      define __LZO_RENAME_B 1
#    endif
#  elif defined(__TSC__) && defined(__OS2__)
#    define __LZO_RENAME_A 1
#  elif defined(__MSDOS__) && defined(__TURBOC__) && (__TURBOC__ < 0x0410)
#    define __LZO_RENAME_A 1
#  elif defined(__PACIFIC__) && defined(DOS)
#    if !defined(__far)
#      define __far far
#    endif
#    if !defined(__near)
#      define __near near
#    endif
#  endif
#  if defined(__LZO_RENAME_A)
#    if !defined(__cdecl)
#      define __cdecl cdecl
#    endif
#    if !defined(__far)
#      define __far far
#    endif
#    if !defined(__huge)
#      define __huge huge
#    endif
#    if !defined(__near)
#      define __near near
#    endif
#    if !defined(__pascal)
#      define __pascal pascal
#    endif
#    if !defined(__huge)
#      define __huge huge
#    endif
#  elif defined(__LZO_RENAME_B)
#    if !defined(__cdecl)
#      define __cdecl _cdecl
#    endif
#    if !defined(__far)
#      define __far _far
#    endif
#    if !defined(__huge)
#      define __huge _huge
#    endif
#    if !defined(__near)
#      define __near _near
#    endif
#    if !defined(__pascal)
#      define __pascal _pascal
#    endif
#  elif (defined(__PUREC__) || defined(__TURBOC__)) && defined(__TOS__)
#    if !defined(__cdecl)
#      define __cdecl cdecl
#    endif
#    if !defined(__pascal)
#      define __pascal pascal
#    endif
#  endif
#  undef __LZO_RENAME_A
#  undef __LZO_RENAME_B
#endif
#if (UINT_MAX == LZO_0xffffL)
#if defined(__AZTEC_C__) && defined(__DOS__)
#  define LZO_BROKEN_CDECL_ALT_SYNTAX 1
#elif defined(_MSC_VER) && defined(MSDOS)
#  if (_MSC_VER < 600)
#    define LZO_BROKEN_INTEGRAL_CONSTANTS 1
#  endif
#  if (_MSC_VER < 700)
#    define LZO_BROKEN_INTEGRAL_PROMOTION 1
#    define LZO_BROKEN_SIZEOF 1
#  endif
#elif defined(__PACIFIC__) && defined(DOS)
#  define LZO_BROKEN_INTEGRAL_CONSTANTS 1
#elif defined(__TURBOC__) && defined(__MSDOS__)
#  if (__TURBOC__ < 0x0150)
#    define LZO_BROKEN_CDECL_ALT_SYNTAX 1
#    define LZO_BROKEN_INTEGRAL_CONSTANTS 1
#    define LZO_BROKEN_INTEGRAL_PROMOTION 1
#  endif
#  if (__TURBOC__ < 0x0200)
#    define LZO_BROKEN_SIZEOF 1
#  endif
#  if (__TURBOC__ < 0x0400) && defined(__cplusplus)
#    define LZO_BROKEN_CDECL_ALT_SYNTAX 1
#  endif
#elif (defined(__PUREC__) || defined(__TURBOC__)) && defined(__TOS__)
#  define LZO_BROKEN_CDECL_ALT_SYNTAX 1
#  define LZO_BROKEN_SIZEOF 1
#endif
#endif
#if defined(__WATCOMC__) && (__WATCOMC__ < 900)
#  define LZO_BROKEN_INTEGRAL_CONSTANTS 1
#endif
#if defined(_CRAY) && defined(_CRAY1)
#  define LZO_BROKEN_SIGNED_RIGHT_SHIFT 1
#endif
#define LZO_PP_STRINGIZE(x)             #x
#define LZO_PP_MACRO_EXPAND(x)          LZO_PP_STRINGIZE(x)
#define LZO_PP_CONCAT2(a,b)             a ## b
#define LZO_PP_CONCAT3(a,b,c)           a ## b ## c
#define LZO_PP_CONCAT4(a,b,c,d)         a ## b ## c ## d
#define LZO_PP_CONCAT5(a,b,c,d,e)       a ## b ## c ## d ## e
#define LZO_PP_ECONCAT2(a,b)            LZO_PP_CONCAT2(a,b)
#define LZO_PP_ECONCAT3(a,b,c)          LZO_PP_CONCAT3(a,b,c)
#define LZO_PP_ECONCAT4(a,b,c,d)        LZO_PP_CONCAT4(a,b,c,d)
#define LZO_PP_ECONCAT5(a,b,c,d,e)      LZO_PP_CONCAT5(a,b,c,d,e)
#if 1
#define LZO_CPP_STRINGIZE(x)            #x
#define LZO_CPP_MACRO_EXPAND(x)         LZO_CPP_STRINGIZE(x)
#define LZO_CPP_CONCAT2(a,b)            a ## b
#define LZO_CPP_CONCAT3(a,b,c)          a ## b ## c
#define LZO_CPP_CONCAT4(a,b,c,d)        a ## b ## c ## d
#define LZO_CPP_CONCAT5(a,b,c,d,e)      a ## b ## c ## d ## e
#define LZO_CPP_ECONCAT2(a,b)           LZO_CPP_CONCAT2(a,b)
#define LZO_CPP_ECONCAT3(a,b,c)         LZO_CPP_CONCAT3(a,b,c)
#define LZO_CPP_ECONCAT4(a,b,c,d)       LZO_CPP_CONCAT4(a,b,c,d)
#define LZO_CPP_ECONCAT5(a,b,c,d,e)     LZO_CPP_CONCAT5(a,b,c,d,e)
#endif
#define __LZO_MASK_GEN(o,b)     (((((o) << ((b)-1)) - (o)) << 1) + (o))
#if 1 && defined(__cplusplus)
#  if !defined(__STDC_CONSTANT_MACROS)
#    define __STDC_CONSTANT_MACROS 1
#  endif
#  if !defined(__STDC_LIMIT_MACROS)
#    define __STDC_LIMIT_MACROS 1
#  endif
#endif
#if defined(__cplusplus)
#  define LZO_EXTERN_C extern "C"
#else
#  define LZO_EXTERN_C extern
#endif
#if !defined(__LZO_OS_OVERRIDE)
#if defined(LZO_OS_FREESTANDING)
#  define LZO_INFO_OS           "freestanding"
#elif defined(LZO_OS_EMBEDDED)
#  define LZO_INFO_OS           "embedded"
#elif 1 && defined(__IAR_SYSTEMS_ICC__)
#  define LZO_OS_EMBEDDED       1
#  define LZO_INFO_OS           "embedded"
#elif defined(__CYGWIN__) && defined(__GNUC__)
#  define LZO_OS_CYGWIN         1
#  define LZO_INFO_OS           "cygwin"
#elif defined(__EMX__) && defined(__GNUC__)
#  define LZO_OS_EMX            1
#  define LZO_INFO_OS           "emx"
#elif defined(__BEOS__)
#  define LZO_OS_BEOS           1
#  define LZO_INFO_OS           "beos"
#elif defined(__Lynx__)
#  define LZO_OS_LYNXOS         1
#  define LZO_INFO_OS           "lynxos"
#elif defined(__OS400__)
#  define LZO_OS_OS400          1
#  define LZO_INFO_OS           "os400"
#elif defined(__QNX__)
#  define LZO_OS_QNX            1
#  define LZO_INFO_OS           "qnx"
#elif defined(__BORLANDC__) && defined(__DPMI32__) && (__BORLANDC__ >= 0x0460)
#  define LZO_OS_DOS32          1
#  define LZO_INFO_OS           "dos32"
#elif defined(__BORLANDC__) && defined(__DPMI16__)
#  define LZO_OS_DOS16          1
#  define LZO_INFO_OS           "dos16"
#elif defined(__ZTC__) && defined(DOS386)
#  define LZO_OS_DOS32          1
#  define LZO_INFO_OS           "dos32"
#elif defined(__OS2__) || defined(__OS2V2__)
#  if (UINT_MAX == LZO_0xffffL)
#    define LZO_OS_OS216        1
#    define LZO_INFO_OS         "os216"
#  elif (UINT_MAX == LZO_0xffffffffL)
#    define LZO_OS_OS2          1
#    define LZO_INFO_OS         "os2"
#  else
#    error "check your limits.h header"
#  endif
#elif defined(__WIN64__) || defined(_WIN64) || defined(WIN64)
#  define LZO_OS_WIN64          1
#  define LZO_INFO_OS           "win64"
#elif defined(__WIN32__) || defined(_WIN32) || defined(WIN32) || defined(__WINDOWS_386__)
#  define LZO_OS_WIN32          1
#  define LZO_INFO_OS           "win32"
#elif defined(__MWERKS__) && defined(__INTEL__)
#  define LZO_OS_WIN32          1
#  define LZO_INFO_OS           "win32"
#elif defined(__WINDOWS__) || defined(_WINDOWS) || defined(_Windows)
#  if (UINT_MAX == LZO_0xffffL)
#    define LZO_OS_WIN16        1
#    define LZO_INFO_OS         "win16"
#  elif (UINT_MAX == LZO_0xffffffffL)
#    define LZO_OS_WIN32        1
#    define LZO_INFO_OS         "win32"
#  else
#    error "check your limits.h header"
#  endif
#elif defined(__DOS__) || defined(__MSDOS__) || defined(_MSDOS) || defined(MSDOS) || (defined(__PACIFIC__) && defined(DOS))
#  if (UINT_MAX == LZO_0xffffL)
#    define LZO_OS_DOS16        1
#    define LZO_INFO_OS         "dos16"
#  elif (UINT_MAX == LZO_0xffffffffL)
#    define LZO_OS_DOS32        1
#    define LZO_INFO_OS         "dos32"
#  else
#    error "check your limits.h header"
#  endif
#elif defined(__WATCOMC__)
#  if defined(__NT__) && (UINT_MAX == LZO_0xffffL)
#    define LZO_OS_DOS16        1
#    define LZO_INFO_OS         "dos16"
#  elif defined(__NT__) && (__WATCOMC__ < 1100)
#    define LZO_OS_WIN32        1
#    define LZO_INFO_OS         "win32"
#  elif defined(__linux__) || defined(__LINUX__)
#    define LZO_OS_POSIX        1
#    define LZO_INFO_OS         "posix"
#  else
#    error "please specify a target using the -bt compiler option"
#  endif
#elif defined(__palmos__)
#  define LZO_OS_PALMOS         1
#  define LZO_INFO_OS           "palmos"
#elif defined(__TOS__) || defined(__atarist__)
#  define LZO_OS_TOS            1
#  define LZO_INFO_OS           "tos"
#elif defined(macintosh) && !defined(__ppc__)
#  define LZO_OS_MACCLASSIC     1
#  define LZO_INFO_OS           "macclassic"
#elif defined(__VMS)
#  define LZO_OS_VMS            1
#  define LZO_INFO_OS           "vms"
#elif ((defined(__mips__) && defined(__R5900__)) || defined(__MIPS_PSX2__))
#  define LZO_OS_CONSOLE        1
#  define LZO_OS_CONSOLE_PS2    1
#  define LZO_INFO_OS           "console"
#  define LZO_INFO_OS_CONSOLE   "ps2"
#elif (defined(__mips__) && defined(__psp__))
#  define LZO_OS_CONSOLE        1
#  define LZO_OS_CONSOLE_PSP    1
#  define LZO_INFO_OS           "console"
#  define LZO_INFO_OS_CONSOLE   "psp"
#else
#  define LZO_OS_POSIX          1
#  define LZO_INFO_OS           "posix"
#endif
#if (LZO_OS_POSIX)
#  if defined(_AIX) || defined(__AIX__) || defined(__aix__)
#    define LZO_OS_POSIX_AIX        1
#    define LZO_INFO_OS_POSIX       "aix"
#  elif defined(__FreeBSD__)
#    define LZO_OS_POSIX_FREEBSD    1
#    define LZO_INFO_OS_POSIX       "freebsd"
#  elif defined(__hpux__) || defined(__hpux)
#    define LZO_OS_POSIX_HPUX       1
#    define LZO_INFO_OS_POSIX       "hpux"
#  elif defined(__INTERIX)
#    define LZO_OS_POSIX_INTERIX    1
#    define LZO_INFO_OS_POSIX       "interix"
#  elif defined(__IRIX__) || defined(__irix__)
#    define LZO_OS_POSIX_IRIX       1
#    define LZO_INFO_OS_POSIX       "irix"
#  elif defined(__linux__) || defined(__linux) || defined(__LINUX__)
#    define LZO_OS_POSIX_LINUX      1
#    define LZO_INFO_OS_POSIX       "linux"
#  elif defined(__APPLE__) || defined(__MACOS__)
#    define LZO_OS_POSIX_MACOSX     1
#    define LZO_INFO_OS_POSIX       "macosx"
#  elif defined(__minix__) || defined(__minix)
#    define LZO_OS_POSIX_MINIX      1
#    define LZO_INFO_OS_POSIX       "minix"
#  elif defined(__NetBSD__)
#    define LZO_OS_POSIX_NETBSD     1
#    define LZO_INFO_OS_POSIX       "netbsd"
#  elif defined(__OpenBSD__)
#    define LZO_OS_POSIX_OPENBSD    1
#    define LZO_INFO_OS_POSIX       "openbsd"
#  elif defined(__osf__)
#    define LZO_OS_POSIX_OSF        1
#    define LZO_INFO_OS_POSIX       "osf"
#  elif defined(__solaris__) || defined(__sun)
#    if defined(__SVR4) || defined(__svr4__)
#      define LZO_OS_POSIX_SOLARIS  1
#      define LZO_INFO_OS_POSIX     "solaris"
#    else
#      define LZO_OS_POSIX_SUNOS    1
#      define LZO_INFO_OS_POSIX     "sunos"
#    endif
#  elif defined(__ultrix__) || defined(__ultrix)
#    define LZO_OS_POSIX_ULTRIX     1
#    define LZO_INFO_OS_POSIX       "ultrix"
#  elif defined(_UNICOS)
#    define LZO_OS_POSIX_UNICOS     1
#    define LZO_INFO_OS_POSIX       "unicos"
#  else
#    define LZO_OS_POSIX_UNKNOWN    1
#    define LZO_INFO_OS_POSIX       "unknown"
#  endif
#endif
#endif
#if (LZO_OS_DOS16 || LZO_OS_OS216 || LZO_OS_WIN16)
#  if (UINT_MAX != LZO_0xffffL)
#    error "this should not happen"
#  endif
#  if (ULONG_MAX != LZO_0xffffffffL)
#    error "this should not happen"
#  endif
#endif
#if (LZO_OS_DOS32 || LZO_OS_OS2 || LZO_OS_WIN32 || LZO_OS_WIN64)
#  if (UINT_MAX != LZO_0xffffffffL)
#    error "this should not happen"
#  endif
#  if (ULONG_MAX != LZO_0xffffffffL)
#    error "this should not happen"
#  endif
#endif
#if defined(CIL) && defined(_GNUCC) && defined(__GNUC__)
#  define LZO_CC_CILLY          1
#  define LZO_INFO_CC           "Cilly"
#  if defined(__CILLY__)
#    define LZO_INFO_CCVER      LZO_PP_MACRO_EXPAND(__CILLY__)
#  else
#    define LZO_INFO_CCVER      "unknown"
#  endif
#elif 0 && defined(SDCC) && defined(__VERSION__) && !defined(__GNUC__)
#  define LZO_CC_SDCC           1
#  define LZO_INFO_CC           "sdcc"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(SDCC)
#elif defined(__PATHSCALE__) && defined(__PATHCC_PATCHLEVEL__)
#  define LZO_CC_PATHSCALE      (__PATHCC__ * 0x10000L + __PATHCC_MINOR__ * 0x100 + __PATHCC_PATCHLEVEL__)
#  define LZO_INFO_CC           "Pathscale C"
#  define LZO_INFO_CCVER        __PATHSCALE__
#elif defined(__INTEL_COMPILER)
#  define LZO_CC_INTELC         1
#  define LZO_INFO_CC           "Intel C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__INTEL_COMPILER)
#  if defined(_WIN32) || defined(_WIN64)
#    define LZO_CC_SYNTAX_MSC 1
#  else
#    define LZO_CC_SYNTAX_GNUC 1
#  endif
#elif defined(__POCC__) && defined(_WIN32)
#  define LZO_CC_PELLESC        1
#  define LZO_INFO_CC           "Pelles C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__POCC__)
#elif defined(__llvm__) && defined(__GNUC__) && defined(__VERSION__)
#  if defined(__GNUC_MINOR__) && defined(__GNUC_PATCHLEVEL__)
#    define LZO_CC_LLVM         (__GNUC__ * 0x10000L + __GNUC_MINOR__ * 0x100 + __GNUC_PATCHLEVEL__)
#  else
#    define LZO_CC_LLVM         (__GNUC__ * 0x10000L + __GNUC_MINOR__ * 0x100)
#  endif
#  define LZO_INFO_CC           "llvm-gcc"
#  define LZO_INFO_CCVER        __VERSION__
#elif defined(__GNUC__) && defined(__VERSION__)
#  if defined(__GNUC_MINOR__) && defined(__GNUC_PATCHLEVEL__)
#    define LZO_CC_GNUC         (__GNUC__ * 0x10000L + __GNUC_MINOR__ * 0x100 + __GNUC_PATCHLEVEL__)
#  elif defined(__GNUC_MINOR__)
#    define LZO_CC_GNUC         (__GNUC__ * 0x10000L + __GNUC_MINOR__ * 0x100)
#  else
#    define LZO_CC_GNUC         (__GNUC__ * 0x10000L)
#  endif
#  define LZO_INFO_CC           "gcc"
#  define LZO_INFO_CCVER        __VERSION__
#elif defined(__ACK__) && defined(_ACK)
#  define LZO_CC_ACK            1
#  define LZO_INFO_CC           "Amsterdam Compiler Kit C"
#  define LZO_INFO_CCVER        "unknown"
#elif defined(__AZTEC_C__)
#  define LZO_CC_AZTECC         1
#  define LZO_INFO_CC           "Aztec C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__AZTEC_C__)
#elif defined(__BORLANDC__)
#  define LZO_CC_BORLANDC       1
#  define LZO_INFO_CC           "Borland C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__BORLANDC__)
#elif defined(_CRAYC) && defined(_RELEASE)
#  define LZO_CC_CRAYC          1
#  define LZO_INFO_CC           "Cray C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(_RELEASE)
#elif defined(__DMC__) && defined(__SC__)
#  define LZO_CC_DMC            1
#  define LZO_INFO_CC           "Digital Mars C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__DMC__)
#elif defined(__DECC)
#  define LZO_CC_DECC           1
#  define LZO_INFO_CC           "DEC C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__DECC)
#elif defined(__HIGHC__)
#  define LZO_CC_HIGHC          1
#  define LZO_INFO_CC           "MetaWare High C"
#  define LZO_INFO_CCVER        "unknown"
#elif defined(__IAR_SYSTEMS_ICC__)
#  define LZO_CC_IARC           1
#  define LZO_INFO_CC           "IAR C"
#  if defined(__VER__)
#    define LZO_INFO_CCVER      LZO_PP_MACRO_EXPAND(__VER__)
#  else
#    define LZO_INFO_CCVER      "unknown"
#  endif
#elif defined(__IBMC__)
#  define LZO_CC_IBMC           1
#  define LZO_INFO_CC           "IBM C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__IBMC__)
#elif defined(__KEIL__) && defined(__C166__)
#  define LZO_CC_KEILC          1
#  define LZO_INFO_CC           "Keil C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__C166__)
#elif defined(__LCC__) && defined(_WIN32) && defined(__LCCOPTIMLEVEL)
#  define LZO_CC_LCCWIN32       1
#  define LZO_INFO_CC           "lcc-win32"
#  define LZO_INFO_CCVER        "unknown"
#elif defined(__LCC__)
#  define LZO_CC_LCC            1
#  define LZO_INFO_CC           "lcc"
#  if defined(__LCC_VERSION__)
#    define LZO_INFO_CCVER      LZO_PP_MACRO_EXPAND(__LCC_VERSION__)
#  else
#    define LZO_INFO_CCVER      "unknown"
#  endif
#elif defined(_MSC_VER)
#  define LZO_CC_MSC            1
#  define LZO_INFO_CC           "Microsoft C"
#  if defined(_MSC_FULL_VER)
#    define LZO_INFO_CCVER      LZO_PP_MACRO_EXPAND(_MSC_VER) "." LZO_PP_MACRO_EXPAND(_MSC_FULL_VER)
#  else
#    define LZO_INFO_CCVER      LZO_PP_MACRO_EXPAND(_MSC_VER)
#  endif
#elif defined(__MWERKS__)
#  define LZO_CC_MWERKS         1
#  define LZO_INFO_CC           "Metrowerks C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__MWERKS__)
#elif (defined(__NDPC__) || defined(__NDPX__)) && defined(__i386)
#  define LZO_CC_NDPC           1
#  define LZO_INFO_CC           "Microway NDP C"
#  define LZO_INFO_CCVER        "unknown"
#elif defined(__PACIFIC__)
#  define LZO_CC_PACIFICC       1
#  define LZO_INFO_CC           "Pacific C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__PACIFIC__)
#elif defined(__PGI) && (defined(__linux__) || defined(__WIN32__))
#  define LZO_CC_PGI            1
#  define LZO_INFO_CC           "Portland Group PGI C"
#  define LZO_INFO_CCVER        "unknown"
#elif defined(__PUREC__) && defined(__TOS__)
#  define LZO_CC_PUREC          1
#  define LZO_INFO_CC           "Pure C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__PUREC__)
#elif defined(__SC__) && defined(__ZTC__)
#  define LZO_CC_SYMANTECC      1
#  define LZO_INFO_CC           "Symantec C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__SC__)
#elif defined(__SUNPRO_C)
#  define LZO_INFO_CC           "SunPro C"
#  if ((__SUNPRO_C)+0 > 0)
#    define LZO_CC_SUNPROC      __SUNPRO_C
#    define LZO_INFO_CCVER      LZO_PP_MACRO_EXPAND(__SUNPRO_C)
#  else
#    define LZO_CC_SUNPROC      1
#    define LZO_INFO_CCVER      "unknown"
#  endif
#elif defined(__SUNPRO_CC)
#  define LZO_INFO_CC           "SunPro C"
#  if ((__SUNPRO_CC)+0 > 0)
#    define LZO_CC_SUNPROC      __SUNPRO_CC
#    define LZO_INFO_CCVER      LZO_PP_MACRO_EXPAND(__SUNPRO_CC)
#  else
#    define LZO_CC_SUNPROC      1
#    define LZO_INFO_CCVER      "unknown"
#  endif
#elif defined(__TINYC__)
#  define LZO_CC_TINYC          1
#  define LZO_INFO_CC           "Tiny C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__TINYC__)
#elif defined(__TSC__)
#  define LZO_CC_TOPSPEEDC      1
#  define LZO_INFO_CC           "TopSpeed C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__TSC__)
#elif defined(__WATCOMC__)
#  define LZO_CC_WATCOMC        1
#  define LZO_INFO_CC           "Watcom C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__WATCOMC__)
#elif defined(__TURBOC__)
#  define LZO_CC_TURBOC         1
#  define LZO_INFO_CC           "Turbo C"
#  define LZO_INFO_CCVER        LZO_PP_MACRO_EXPAND(__TURBOC__)
#elif defined(__ZTC__)
#  define LZO_CC_ZORTECHC       1
#  define LZO_INFO_CC           "Zortech C"
#  if (__ZTC__ == 0x310)
#    define LZO_INFO_CCVER      "0x310"
#  else
#    define LZO_INFO_CCVER      LZO_PP_MACRO_EXPAND(__ZTC__)
#  endif
#else
#  define LZO_CC_UNKNOWN        1
#  define LZO_INFO_CC           "unknown"
#  define LZO_INFO_CCVER        "unknown"
#endif
#if 0 && (LZO_CC_MSC && (_MSC_VER >= 1200)) && !defined(_MSC_FULL_VER)
#  error "LZO_CC_MSC: _MSC_FULL_VER is not defined"
#endif
#if !defined(__LZO_ARCH_OVERRIDE) && !defined(LZO_ARCH_GENERIC) && defined(_CRAY)
#  if (UINT_MAX > LZO_0xffffffffL) && defined(_CRAY)
#    if defined(_CRAYMPP) || defined(_CRAYT3D) || defined(_CRAYT3E)
#      define LZO_ARCH_CRAY_MPP     1
#    elif defined(_CRAY1)
#      define LZO_ARCH_CRAY_PVP     1
#    endif
#  endif
#endif
#if !defined(__LZO_ARCH_OVERRIDE)
#if defined(LZO_ARCH_GENERIC)
#  define LZO_INFO_ARCH             "generic"
#elif (LZO_OS_DOS16 || LZO_OS_OS216 || LZO_OS_WIN16)
#  define LZO_ARCH_I086             1
#  define LZO_ARCH_IA16             1
#  define LZO_INFO_ARCH             "i086"
#elif defined(__alpha__) || defined(__alpha) || defined(_M_ALPHA)
#  define LZO_ARCH_ALPHA            1
#  define LZO_INFO_ARCH             "alpha"
#elif (LZO_ARCH_CRAY_MPP) && (defined(_CRAYT3D) || defined(_CRAYT3E))
#  define LZO_ARCH_ALPHA            1
#  define LZO_INFO_ARCH             "alpha"
#elif defined(__amd64__) || defined(__x86_64__) || defined(_M_AMD64)
#  define LZO_ARCH_AMD64            1
#  define LZO_INFO_ARCH             "amd64"
#elif defined(__thumb__) || (defined(_M_ARM) && defined(_M_THUMB))
#  define LZO_ARCH_ARM              1
#  define LZO_ARCH_ARM_THUMB        1
#  define LZO_INFO_ARCH             "arm_thumb"
#elif defined(__IAR_SYSTEMS_ICC__) && defined(__ICCARM__)
#  define LZO_ARCH_ARM              1
#  if defined(__CPU_MODE__) && ((__CPU_MODE__)+0 == 1)
#    define LZO_ARCH_ARM_THUMB      1
#    define LZO_INFO_ARCH           "arm_thumb"
#  elif defined(__CPU_MODE__) && ((__CPU_MODE__)+0 == 2)
#    define LZO_INFO_ARCH           "arm"
#  else
#    define LZO_INFO_ARCH           "arm"
#  endif
#elif defined(__arm__) || defined(_M_ARM)
#  define LZO_ARCH_ARM              1
#  define LZO_INFO_ARCH             "arm"
#elif (UINT_MAX <= LZO_0xffffL) && defined(__AVR__)
#  define LZO_ARCH_AVR              1
#  define LZO_INFO_ARCH             "avr"
#elif defined(__bfin__)
#  define LZO_ARCH_BLACKFIN         1
#  define LZO_INFO_ARCH             "blackfin"
#elif (UINT_MAX == LZO_0xffffL) && defined(__C166__)
#  define LZO_ARCH_C166             1
#  define LZO_INFO_ARCH             "c166"
#elif defined(__cris__)
#  define LZO_ARCH_CRIS             1
#  define LZO_INFO_ARCH             "cris"
#elif defined(__IAR_SYSTEMS_ICC__) && defined(__ICCEZ80__)
#  define LZO_ARCH_EZ80             1
#  define LZO_INFO_ARCH             "ez80"
#elif defined(__H8300__) || defined(__H8300H__) || defined(__H8300S__) || defined(__H8300SX__)
#  define LZO_ARCH_H8300            1
#  define LZO_INFO_ARCH             "h8300"
#elif defined(__hppa__) || defined(__hppa)
#  define LZO_ARCH_HPPA             1
#  define LZO_INFO_ARCH             "hppa"
#elif defined(__386__) || defined(__i386__) || defined(__i386) || defined(_M_IX86) || defined(_M_I386)
#  define LZO_ARCH_I386             1
#  define LZO_ARCH_IA32             1
#  define LZO_INFO_ARCH             "i386"
#elif (LZO_CC_ZORTECHC && defined(__I86__))
#  define LZO_ARCH_I386             1
#  define LZO_ARCH_IA32             1
#  define LZO_INFO_ARCH             "i386"
#elif (LZO_OS_DOS32 && LZO_CC_HIGHC) && defined(_I386)
#  define LZO_ARCH_I386             1
#  define LZO_ARCH_IA32             1
#  define LZO_INFO_ARCH             "i386"
#elif defined(__ia64__) || defined(__ia64) || defined(_M_IA64)
#  define LZO_ARCH_IA64             1
#  define LZO_INFO_ARCH             "ia64"
#elif (UINT_MAX == LZO_0xffffL) && defined(__m32c__)
#  define LZO_ARCH_M16C             1
#  define LZO_INFO_ARCH             "m16c"
#elif defined(__IAR_SYSTEMS_ICC__) && defined(__ICCM16C__)
#  define LZO_ARCH_M16C             1
#  define LZO_INFO_ARCH             "m16c"
#elif defined(__m32r__)
#  define LZO_ARCH_M32R             1
#  define LZO_INFO_ARCH             "m32r"
#elif (LZO_OS_TOS) || defined(__m68k__) || defined(__m68000__) || defined(__mc68000__) || defined(__mc68020__) || defined(_M_M68K)
#  define LZO_ARCH_M68K             1
#  define LZO_INFO_ARCH             "m68k"
#elif (UINT_MAX == LZO_0xffffL) && defined(__C251__)
#  define LZO_ARCH_MCS251           1
#  define LZO_INFO_ARCH             "mcs251"
#elif (UINT_MAX == LZO_0xffffL) && defined(__C51__)
#  define LZO_ARCH_MCS51            1
#  define LZO_INFO_ARCH             "mcs51"
#elif defined(__IAR_SYSTEMS_ICC__) && defined(__ICC8051__)
#  define LZO_ARCH_MCS51            1
#  define LZO_INFO_ARCH             "mcs51"
#elif defined(__mips__) || defined(__mips) || defined(_MIPS_ARCH) || defined(_M_MRX000)
#  define LZO_ARCH_MIPS             1
#  define LZO_INFO_ARCH             "mips"
#elif (UINT_MAX == LZO_0xffffL) && defined(__MSP430__)
#  define LZO_ARCH_MSP430           1
#  define LZO_INFO_ARCH             "msp430"
#elif defined(__IAR_SYSTEMS_ICC__) && defined(__ICC430__)
#  define LZO_ARCH_MSP430           1
#  define LZO_INFO_ARCH             "msp430"
#elif defined(__powerpc__) || defined(__powerpc) || defined(__ppc__) || defined(__PPC__) || defined(_M_PPC) || defined(_ARCH_PPC) || defined(_ARCH_PWR)
#  define LZO_ARCH_POWERPC          1
#  define LZO_INFO_ARCH             "powerpc"
#elif defined(__s390__) || defined(__s390) || defined(__s390x__) || defined(__s390x)
#  define LZO_ARCH_S390             1
#  define LZO_INFO_ARCH             "s390"
#elif defined(__sh__) || defined(_M_SH)
#  define LZO_ARCH_SH               1
#  define LZO_INFO_ARCH             "sh"
#elif defined(__sparc__) || defined(__sparc) || defined(__sparcv8)
#  define LZO_ARCH_SPARC            1
#  define LZO_INFO_ARCH             "sparc"
#elif defined(__SPU__)
#  define LZO_ARCH_SPU              1
#  define LZO_INFO_ARCH             "spu"
#elif (UINT_MAX == LZO_0xffffL) && defined(__z80)
#  define LZO_ARCH_Z80              1
#  define LZO_INFO_ARCH             "z80"
#elif (LZO_ARCH_CRAY_PVP)
#  if defined(_CRAYSV1)
#    define LZO_ARCH_CRAY_SV1       1
#    define LZO_INFO_ARCH           "cray_sv1"
#  elif (_ADDR64)
#    define LZO_ARCH_CRAY_T90       1
#    define LZO_INFO_ARCH           "cray_t90"
#  elif (_ADDR32)
#    define LZO_ARCH_CRAY_YMP       1
#    define LZO_INFO_ARCH           "cray_ymp"
#  else
#    define LZO_ARCH_CRAY_XMP       1
#    define LZO_INFO_ARCH           "cray_xmp"
#  endif
#else
#  define LZO_ARCH_UNKNOWN          1
#  define LZO_INFO_ARCH             "unknown"
#endif
#endif
#if 1 && (LZO_ARCH_UNKNOWN) && (LZO_OS_DOS32 || LZO_OS_OS2)
#  error "FIXME - missing define for CPU architecture"
#endif
#if 1 && (LZO_ARCH_UNKNOWN) && (LZO_OS_WIN32)
#  error "FIXME - missing WIN32 define for CPU architecture"
#endif
#if 1 && (LZO_ARCH_UNKNOWN) && (LZO_OS_WIN64)
#  error "FIXME - missing WIN64 define for CPU architecture"
#endif
#if (LZO_OS_OS216 || LZO_OS_WIN16)
#  define LZO_ARCH_I086PM           1
#  define LZO_ARCH_IA16PM           1
#elif 1 && (LZO_OS_DOS16 && defined(BLX286))
#  define LZO_ARCH_I086PM           1
#  define LZO_ARCH_IA16PM           1
#elif 1 && (LZO_OS_DOS16 && defined(DOSX286))
#  define LZO_ARCH_I086PM           1
#  define LZO_ARCH_IA16PM           1
#elif 1 && (LZO_OS_DOS16 && LZO_CC_BORLANDC && defined(__DPMI16__))
#  define LZO_ARCH_I086PM           1
#  define LZO_ARCH_IA16PM           1
#endif
#if defined(LZO_ARCH_ARM_THUMB) && !defined(LZO_ARCH_ARM)
#  error "this should not happen"
#endif
#if defined(LZO_ARCH_I086PM) && !defined(LZO_ARCH_I086)
#  error "this should not happen"
#endif
#if (LZO_ARCH_I086)
#  if (UINT_MAX != LZO_0xffffL)
#    error "this should not happen"
#  endif
#  if (ULONG_MAX != LZO_0xffffffffL)
#    error "this should not happen"
#  endif
#endif
#if (LZO_ARCH_I386)
#  if (UINT_MAX != LZO_0xffffL) && defined(__i386_int16__)
#    error "this should not happen"
#  endif
#  if (UINT_MAX != LZO_0xffffffffL) && !defined(__i386_int16__)
#    error "this should not happen"
#  endif
#  if (ULONG_MAX != LZO_0xffffffffL)
#    error "this should not happen"
#  endif
#endif
#if !defined(__LZO_MM_OVERRIDE)
#if (LZO_ARCH_I086)
#if (UINT_MAX != LZO_0xffffL)
#  error "this should not happen"
#endif
#if defined(__TINY__) || defined(M_I86TM) || defined(_M_I86TM)
#  define LZO_MM_TINY           1
#elif defined(__HUGE__) || defined(_HUGE_) || defined(M_I86HM) || defined(_M_I86HM)
#  define LZO_MM_HUGE           1
#elif defined(__SMALL__) || defined(M_I86SM) || defined(_M_I86SM) || defined(SMALL_MODEL)
#  define LZO_MM_SMALL          1
#elif defined(__MEDIUM__) || defined(M_I86MM) || defined(_M_I86MM)
#  define LZO_MM_MEDIUM         1
#elif defined(__COMPACT__) || defined(M_I86CM) || defined(_M_I86CM)
#  define LZO_MM_COMPACT        1
#elif defined(__LARGE__) || defined(M_I86LM) || defined(_M_I86LM) || defined(LARGE_MODEL)
#  define LZO_MM_LARGE          1
#elif (LZO_CC_AZTECC)
#  if defined(_LARGE_CODE) && defined(_LARGE_DATA)
#    define LZO_MM_LARGE        1
#  elif defined(_LARGE_CODE)
#    define LZO_MM_MEDIUM       1
#  elif defined(_LARGE_DATA)
#    define LZO_MM_COMPACT      1
#  else
#    define LZO_MM_SMALL        1
#  endif
#elif (LZO_CC_ZORTECHC && defined(__VCM__))
#  define LZO_MM_LARGE          1
#else
#  error "unknown memory model"
#endif
#if (LZO_OS_DOS16 || LZO_OS_OS216 || LZO_OS_WIN16)
#define LZO_HAVE_MM_HUGE_PTR        1
#define LZO_HAVE_MM_HUGE_ARRAY      1
#if (LZO_MM_TINY)
#  undef LZO_HAVE_MM_HUGE_ARRAY
#endif
#if (LZO_CC_AZTECC || LZO_CC_PACIFICC || LZO_CC_ZORTECHC)
#  undef LZO_HAVE_MM_HUGE_PTR
#  undef LZO_HAVE_MM_HUGE_ARRAY
#elif (LZO_CC_DMC || LZO_CC_SYMANTECC)
#  undef LZO_HAVE_MM_HUGE_ARRAY
#elif (LZO_CC_MSC && defined(_QC))
#  undef LZO_HAVE_MM_HUGE_ARRAY
#  if (_MSC_VER < 600)
#    undef LZO_HAVE_MM_HUGE_PTR
#  endif
#elif (LZO_CC_TURBOC && (__TURBOC__ < 0x0295))
#  undef LZO_HAVE_MM_HUGE_ARRAY
#endif
#if (LZO_ARCH_I086PM) && !defined(LZO_HAVE_MM_HUGE_PTR)
#  if (LZO_OS_DOS16)
#    error "this should not happen"
#  elif (LZO_CC_ZORTECHC)
#  else
#    error "this should not happen"
#  endif
#endif
#ifdef __cplusplus
extern "C" {
#endif
#if (LZO_CC_BORLANDC && (__BORLANDC__ >= 0x0200))
   extern void __near __cdecl _AHSHIFT(void);
#  define LZO_MM_AHSHIFT      ((unsigned) _AHSHIFT)
#elif (LZO_CC_DMC || LZO_CC_SYMANTECC || LZO_CC_ZORTECHC)
   extern void __near __cdecl _AHSHIFT(void);
#  define LZO_MM_AHSHIFT      ((unsigned) _AHSHIFT)
#elif (LZO_CC_MSC || LZO_CC_TOPSPEEDC)
   extern void __near __cdecl _AHSHIFT(void);
#  define LZO_MM_AHSHIFT      ((unsigned) _AHSHIFT)
#elif (LZO_CC_TURBOC && (__TURBOC__ >= 0x0295))
   extern void __near __cdecl _AHSHIFT(void);
#  define LZO_MM_AHSHIFT      ((unsigned) _AHSHIFT)
#elif ((LZO_CC_AZTECC || LZO_CC_PACIFICC || LZO_CC_TURBOC) && LZO_OS_DOS16)
#  define LZO_MM_AHSHIFT      12
#elif (LZO_CC_WATCOMC)
   extern unsigned char _HShift;
#  define LZO_MM_AHSHIFT      ((unsigned) _HShift)
#else
#  error "FIXME - implement LZO_MM_AHSHIFT"
#endif
#ifdef __cplusplus
}
#endif
#endif
#elif (LZO_ARCH_C166)
#if !defined(__MODEL__)
#  error "FIXME - C166 __MODEL__"
#elif ((__MODEL__) == 0)
#  define LZO_MM_SMALL          1
#elif ((__MODEL__) == 1)
#  define LZO_MM_SMALL          1
#elif ((__MODEL__) == 2)
#  define LZO_MM_LARGE          1
#elif ((__MODEL__) == 3)
#  define LZO_MM_TINY           1
#elif ((__MODEL__) == 4)
#  define LZO_MM_XTINY          1
#elif ((__MODEL__) == 5)
#  define LZO_MM_XSMALL         1
#else
#  error "FIXME - C166 __MODEL__"
#endif
#elif (LZO_ARCH_MCS251)
#if !defined(__MODEL__)
#  error "FIXME - MCS251 __MODEL__"
#elif ((__MODEL__) == 0)
#  define LZO_MM_SMALL          1
#elif ((__MODEL__) == 2)
#  define LZO_MM_LARGE          1
#elif ((__MODEL__) == 3)
#  define LZO_MM_TINY           1
#elif ((__MODEL__) == 4)
#  define LZO_MM_XTINY          1
#elif ((__MODEL__) == 5)
#  define LZO_MM_XSMALL         1
#else
#  error "FIXME - MCS251 __MODEL__"
#endif
#elif (LZO_ARCH_MCS51)
#if !defined(__MODEL__)
#  error "FIXME - MCS51 __MODEL__"
#elif ((__MODEL__) == 1)
#  define LZO_MM_SMALL          1
#elif ((__MODEL__) == 2)
#  define LZO_MM_LARGE          1
#elif ((__MODEL__) == 3)
#  define LZO_MM_TINY           1
#elif ((__MODEL__) == 4)
#  define LZO_MM_XTINY          1
#elif ((__MODEL__) == 5)
#  define LZO_MM_XSMALL         1
#else
#  error "FIXME - MCS51 __MODEL__"
#endif
#elif (LZO_ARCH_CRAY_PVP)
#  define LZO_MM_PVP            1
#else
#  define LZO_MM_FLAT           1
#endif
#if (LZO_MM_COMPACT)
#  define LZO_INFO_MM           "compact"
#elif (LZO_MM_FLAT)
#  define LZO_INFO_MM           "flat"
#elif (LZO_MM_HUGE)
#  define LZO_INFO_MM           "huge"
#elif (LZO_MM_LARGE)
#  define LZO_INFO_MM           "large"
#elif (LZO_MM_MEDIUM)
#  define LZO_INFO_MM           "medium"
#elif (LZO_MM_PVP)
#  define LZO_INFO_MM           "pvp"
#elif (LZO_MM_SMALL)
#  define LZO_INFO_MM           "small"
#elif (LZO_MM_TINY)
#  define LZO_INFO_MM           "tiny"
#else
#  error "unknown memory model"
#endif
#endif
#if defined(SIZEOF_SHORT)
#  define LZO_SIZEOF_SHORT          (SIZEOF_SHORT)
#endif
#if defined(SIZEOF_INT)
#  define LZO_SIZEOF_INT            (SIZEOF_INT)
#endif
#if defined(SIZEOF_LONG)
#  define LZO_SIZEOF_LONG           (SIZEOF_LONG)
#endif
#if defined(SIZEOF_LONG_LONG)
#  define LZO_SIZEOF_LONG_LONG      (SIZEOF_LONG_LONG)
#endif
#if defined(SIZEOF___INT16)
#  define LZO_SIZEOF___INT16        (SIZEOF___INT16)
#endif
#if defined(SIZEOF___INT32)
#  define LZO_SIZEOF___INT32        (SIZEOF___INT32)
#endif
#if defined(SIZEOF___INT64)
#  define LZO_SIZEOF___INT64        (SIZEOF___INT64)
#endif
#if defined(SIZEOF_VOID_P)
#  define LZO_SIZEOF_VOID_P         (SIZEOF_VOID_P)
#endif
#if defined(SIZEOF_SIZE_T)
#  define LZO_SIZEOF_SIZE_T         (SIZEOF_SIZE_T)
#endif
#if defined(SIZEOF_PTRDIFF_T)
#  define LZO_SIZEOF_PTRDIFF_T      (SIZEOF_PTRDIFF_T)
#endif
#define __LZO_LSR(x,b)    (((x)+0ul) >> (b))
#if !defined(LZO_SIZEOF_SHORT)
#  if (LZO_ARCH_CRAY_PVP)
#    define LZO_SIZEOF_SHORT        8
#  elif (USHRT_MAX == LZO_0xffffL)
#    define LZO_SIZEOF_SHORT        2
#  elif (__LZO_LSR(USHRT_MAX,7) == 1)
#    define LZO_SIZEOF_SHORT        1
#  elif (__LZO_LSR(USHRT_MAX,15) == 1)
#    define LZO_SIZEOF_SHORT        2
#  elif (__LZO_LSR(USHRT_MAX,31) == 1)
#    define LZO_SIZEOF_SHORT        4
#  elif (__LZO_LSR(USHRT_MAX,63) == 1)
#    define LZO_SIZEOF_SHORT        8
#  elif (__LZO_LSR(USHRT_MAX,127) == 1)
#    define LZO_SIZEOF_SHORT        16
#  else
#    error "LZO_SIZEOF_SHORT"
#  endif
#endif
#if !defined(LZO_SIZEOF_INT)
#  if (LZO_ARCH_CRAY_PVP)
#    define LZO_SIZEOF_INT          8
#  elif (UINT_MAX == LZO_0xffffL)
#    define LZO_SIZEOF_INT          2
#  elif (UINT_MAX == LZO_0xffffffffL)
#    define LZO_SIZEOF_INT          4
#  elif (__LZO_LSR(UINT_MAX,7) == 1)
#    define LZO_SIZEOF_INT          1
#  elif (__LZO_LSR(UINT_MAX,15) == 1)
#    define LZO_SIZEOF_INT          2
#  elif (__LZO_LSR(UINT_MAX,31) == 1)
#    define LZO_SIZEOF_INT          4
#  elif (__LZO_LSR(UINT_MAX,63) == 1)
#    define LZO_SIZEOF_INT          8
#  elif (__LZO_LSR(UINT_MAX,127) == 1)
#    define LZO_SIZEOF_INT          16
#  else
#    error "LZO_SIZEOF_INT"
#  endif
#endif
#if !defined(LZO_SIZEOF_LONG)
#  if (ULONG_MAX == LZO_0xffffffffL)
#    define LZO_SIZEOF_LONG         4
#  elif (__LZO_LSR(ULONG_MAX,7) == 1)
#    define LZO_SIZEOF_LONG         1
#  elif (__LZO_LSR(ULONG_MAX,15) == 1)
#    define LZO_SIZEOF_LONG         2
#  elif (__LZO_LSR(ULONG_MAX,31) == 1)
#    define LZO_SIZEOF_LONG         4
#  elif (__LZO_LSR(ULONG_MAX,63) == 1)
#    define LZO_SIZEOF_LONG         8
#  elif (__LZO_LSR(ULONG_MAX,127) == 1)
#    define LZO_SIZEOF_LONG         16
#  else
#    error "LZO_SIZEOF_LONG"
#  endif
#endif
#if !defined(LZO_SIZEOF_LONG_LONG) && !defined(LZO_SIZEOF___INT64)
#if (LZO_SIZEOF_LONG > 0 && LZO_SIZEOF_LONG < 8)
#  if defined(__LONG_MAX__) && defined(__LONG_LONG_MAX__)
#    if (LZO_CC_GNUC >= 0x030300ul)
#      if ((__LONG_MAX__)+0 == (__LONG_LONG_MAX__)+0)
#        define LZO_SIZEOF_LONG_LONG      LZO_SIZEOF_LONG
#      elif (__LZO_LSR(__LONG_LONG_MAX__,30) == 1)
#        define LZO_SIZEOF_LONG_LONG      4
#      endif
#    endif
#  endif
#endif
#endif
#if !defined(LZO_SIZEOF_LONG_LONG) && !defined(LZO_SIZEOF___INT64)
#if (LZO_SIZEOF_LONG > 0 && LZO_SIZEOF_LONG < 8)
#if (LZO_ARCH_I086 && LZO_CC_DMC)
#elif (LZO_CC_CILLY) && defined(__GNUC__)
#  define LZO_SIZEOF_LONG_LONG      8
#elif (LZO_CC_GNUC || LZO_CC_LLVM || LZO_CC_PATHSCALE)
#  define LZO_SIZEOF_LONG_LONG      8
#elif ((LZO_OS_WIN32 || LZO_OS_WIN64 || defined(_WIN32)) && LZO_CC_MSC && (_MSC_VER >= 1400))
#  define LZO_SIZEOF_LONG_LONG      8
#elif (LZO_OS_WIN64 || defined(_WIN64))
#  define LZO_SIZEOF___INT64        8
#elif (LZO_ARCH_I386 && (LZO_CC_DMC))
#  define LZO_SIZEOF_LONG_LONG      8
#elif (LZO_ARCH_I386 && (LZO_CC_SYMANTECC && (__SC__ >= 0x700)))
#  define LZO_SIZEOF_LONG_LONG      8
#elif (LZO_ARCH_I386 && (LZO_CC_INTELC && defined(__linux__)))
#  define LZO_SIZEOF_LONG_LONG      8
#elif (LZO_ARCH_I386 && (LZO_CC_MWERKS || LZO_CC_PELLESC || LZO_CC_PGI || LZO_CC_SUNPROC))
#  define LZO_SIZEOF_LONG_LONG      8
#elif (LZO_ARCH_I386 && (LZO_CC_INTELC || LZO_CC_MSC))
#  define LZO_SIZEOF___INT64        8
#elif ((LZO_OS_WIN32 || defined(_WIN32)) && (LZO_CC_MSC))
#  define LZO_SIZEOF___INT64        8
#elif (LZO_ARCH_I386 && (LZO_CC_BORLANDC && (__BORLANDC__ >= 0x0520)))
#  define LZO_SIZEOF___INT64        8
#elif (LZO_ARCH_I386 && (LZO_CC_WATCOMC && (__WATCOMC__ >= 1100)))
#  define LZO_SIZEOF___INT64        8
#elif (LZO_CC_WATCOMC && defined(_INTEGRAL_MAX_BITS) && (_INTEGRAL_MAX_BITS == 64))
#  define LZO_SIZEOF___INT64        8
#elif (LZO_OS_OS400 || defined(__OS400__)) && defined(__LLP64_IFC__)
#  define LZO_SIZEOF_LONG_LONG      8
#elif (defined(__vms) || defined(__VMS)) && (__INITIAL_POINTER_SIZE+0 == 64)
#  define LZO_SIZEOF_LONG_LONG      8
#elif (LZO_CC_SDCC) && (LZO_SIZEOF_INT == 2)
#elif 1 && defined(__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L)
#  define LZO_SIZEOF_LONG_LONG      8
#endif
#endif
#endif
#if defined(__cplusplus) && defined(LZO_CC_GNUC)
#  if (LZO_CC_GNUC < 0x020800ul)
#    undef LZO_SIZEOF_LONG_LONG
#  endif
#endif
#if defined(LZO_CFG_NO_LONG_LONG) || defined(__NO_LONG_LONG)
#  undef LZO_SIZEOF_LONG_LONG
#endif
#if !defined(LZO_SIZEOF_VOID_P)
#if (LZO_ARCH_I086)
#  define __LZO_WORDSIZE            2
#  if (LZO_MM_TINY || LZO_MM_SMALL || LZO_MM_MEDIUM)
#    define LZO_SIZEOF_VOID_P       2
#  elif (LZO_MM_COMPACT || LZO_MM_LARGE || LZO_MM_HUGE)
#    define LZO_SIZEOF_VOID_P       4
#  else
#    error "LZO_MM"
#  endif
#elif (LZO_ARCH_AVR || LZO_ARCH_Z80)
#  define __LZO_WORDSIZE            1
#  define LZO_SIZEOF_VOID_P         2
#elif (LZO_ARCH_C166 || LZO_ARCH_MCS51 || LZO_ARCH_MCS251 || LZO_ARCH_MSP430)
#  define LZO_SIZEOF_VOID_P         2
#elif (LZO_ARCH_H8300)
#  if defined(__NORMAL_MODE__)
#    define __LZO_WORDSIZE          4
#    define LZO_SIZEOF_VOID_P       2
#  elif defined(__H8300H__) || defined(__H8300S__) || defined(__H8300SX__)
#    define __LZO_WORDSIZE          4
#    define LZO_SIZEOF_VOID_P       4
#  else
#    define __LZO_WORDSIZE          2
#    define LZO_SIZEOF_VOID_P       2
#  endif
#  if (LZO_CC_GNUC && (LZO_CC_GNUC < 0x040000ul)) && (LZO_SIZEOF_INT == 4)
#    define LZO_SIZEOF_SIZE_T       LZO_SIZEOF_INT
#    define LZO_SIZEOF_PTRDIFF_T    LZO_SIZEOF_INT
#  endif
#elif (LZO_ARCH_M16C)
#  define __LZO_WORDSIZE            2
#  if defined(__m32c_cpu__) || defined(__m32cm_cpu__)
#    define LZO_SIZEOF_VOID_P       4
#  else
#    define LZO_SIZEOF_VOID_P       2
#  endif
#elif (LZO_SIZEOF_LONG == 8) && ((defined(__mips__) && defined(__R5900__)) || defined(__MIPS_PSX2__))
#  define __LZO_WORDSIZE            8
#  define LZO_SIZEOF_VOID_P         4
#elif defined(__LLP64__) || defined(__LLP64) || defined(_LLP64) || defined(_WIN64)
#  define __LZO_WORDSIZE            8
#  define LZO_SIZEOF_VOID_P         8
#elif (LZO_OS_OS400 || defined(__OS400__)) && defined(__LLP64_IFC__)
#  define LZO_SIZEOF_VOID_P         LZO_SIZEOF_LONG
#  define LZO_SIZEOF_SIZE_T         LZO_SIZEOF_LONG
#  define LZO_SIZEOF_PTRDIFF_T      LZO_SIZEOF_LONG
#elif (LZO_OS_OS400 || defined(__OS400__))
#  define __LZO_WORDSIZE            LZO_SIZEOF_LONG
#  define LZO_SIZEOF_VOID_P         16
#  define LZO_SIZEOF_SIZE_T         LZO_SIZEOF_LONG
#  define LZO_SIZEOF_PTRDIFF_T      LZO_SIZEOF_LONG
#elif (defined(__vms) || defined(__VMS)) && (__INITIAL_POINTER_SIZE+0 == 64)
#  define LZO_SIZEOF_VOID_P         8
#  define LZO_SIZEOF_SIZE_T         LZO_SIZEOF_LONG
#  define LZO_SIZEOF_PTRDIFF_T      LZO_SIZEOF_LONG
#elif (LZO_ARCH_SPU)
# if 0
#  define __LZO_WORDSIZE            16
# endif
#  define LZO_SIZEOF_VOID_P         4
#else
#  define LZO_SIZEOF_VOID_P         LZO_SIZEOF_LONG
#endif
#endif
#if !defined(LZO_WORDSIZE)
#  if defined(__LZO_WORDSIZE)
#    define LZO_WORDSIZE            __LZO_WORDSIZE
#  else
#    define LZO_WORDSIZE            LZO_SIZEOF_VOID_P
#  endif
#endif
#if !defined(LZO_SIZEOF_SIZE_T)
#if (LZO_ARCH_I086 || LZO_ARCH_M16C)
#  define LZO_SIZEOF_SIZE_T         2
#else
#  define LZO_SIZEOF_SIZE_T         LZO_SIZEOF_VOID_P
#endif
#endif
#if !defined(LZO_SIZEOF_PTRDIFF_T)
#if (LZO_ARCH_I086)
#  if (LZO_MM_TINY || LZO_MM_SMALL || LZO_MM_MEDIUM || LZO_MM_HUGE)
#    define LZO_SIZEOF_PTRDIFF_T    LZO_SIZEOF_VOID_P
#  elif (LZO_MM_COMPACT || LZO_MM_LARGE)
#    if (LZO_CC_BORLANDC || LZO_CC_TURBOC)
#      define LZO_SIZEOF_PTRDIFF_T  4
#    else
#      define LZO_SIZEOF_PTRDIFF_T  2
#    endif
#  else
#    error "LZO_MM"
#  endif
#else
#  define LZO_SIZEOF_PTRDIFF_T      LZO_SIZEOF_SIZE_T
#endif
#endif
#if defined(LZO_ABI_NEUTRAL_ENDIAN)
#  undef LZO_ABI_BIG_ENDIAN
#  undef LZO_ABI_LITTLE_ENDIAN
#elif !defined(LZO_ABI_BIG_ENDIAN) && !defined(LZO_ABI_LITTLE_ENDIAN)
#if (LZO_ARCH_ALPHA) && (LZO_ARCH_CRAY_MPP)
#  define LZO_ABI_BIG_ENDIAN        1
#elif (LZO_ARCH_ALPHA || LZO_ARCH_AMD64 || LZO_ARCH_BLACKFIN || LZO_ARCH_CRIS || LZO_ARCH_I086 || LZO_ARCH_I386 || LZO_ARCH_MSP430)
#  define LZO_ABI_LITTLE_ENDIAN     1
#elif (LZO_ARCH_M68K || LZO_ARCH_S390)
#  define LZO_ABI_BIG_ENDIAN        1
#elif 1 && defined(__IAR_SYSTEMS_ICC__) && defined(__LITTLE_ENDIAN__)
#  if (__LITTLE_ENDIAN__ == 1)
#    define LZO_ABI_LITTLE_ENDIAN   1
#  else
#    define LZO_ABI_BIG_ENDIAN      1
#  endif
#elif 1 && defined(__BIG_ENDIAN__) && !defined(__LITTLE_ENDIAN__)
#  define LZO_ABI_BIG_ENDIAN        1
#elif 1 && defined(__LITTLE_ENDIAN__) && !defined(__BIG_ENDIAN__)
#  define LZO_ABI_LITTLE_ENDIAN     1
#elif 1 && (LZO_ARCH_ARM) && defined(__ARMEB__) && !defined(__ARMEL__)
#  define LZO_ABI_BIG_ENDIAN        1
#elif 1 && (LZO_ARCH_ARM) && defined(__ARMEL__) && !defined(__ARMEB__)
#  define LZO_ABI_LITTLE_ENDIAN     1
#elif 1 && (LZO_ARCH_MIPS) && defined(__MIPSEB__) && !defined(__MIPSEL__)
#  define LZO_ABI_BIG_ENDIAN        1
#elif 1 && (LZO_ARCH_MIPS) && defined(__MIPSEL__) && !defined(__MIPSEB__)
#  define LZO_ABI_LITTLE_ENDIAN     1
#endif
#endif
#if defined(LZO_ABI_BIG_ENDIAN) && defined(LZO_ABI_LITTLE_ENDIAN)
#  error "this should not happen"
#endif
#if defined(LZO_ABI_BIG_ENDIAN)
#  define LZO_INFO_ABI_ENDIAN       "be"
#elif defined(LZO_ABI_LITTLE_ENDIAN)
#  define LZO_INFO_ABI_ENDIAN       "le"
#elif defined(LZO_ABI_NEUTRAL_ENDIAN)
#  define LZO_INFO_ABI_ENDIAN       "neutral"
#endif
#if (LZO_SIZEOF_INT == 1 && LZO_SIZEOF_LONG == 2 && LZO_SIZEOF_VOID_P == 2)
#  define LZO_ABI_I8LP16         1
#  define LZO_INFO_ABI_PM       "i8lp16"
#elif (LZO_SIZEOF_INT == 2 && LZO_SIZEOF_LONG == 2 && LZO_SIZEOF_VOID_P == 2)
#  define LZO_ABI_ILP16         1
#  define LZO_INFO_ABI_PM       "ilp16"
#elif (LZO_SIZEOF_INT == 4 && LZO_SIZEOF_LONG == 4 && LZO_SIZEOF_VOID_P == 4)
#  define LZO_ABI_ILP32         1
#  define LZO_INFO_ABI_PM       "ilp32"
#elif (LZO_SIZEOF_INT == 4 && LZO_SIZEOF_LONG == 4 && LZO_SIZEOF_VOID_P == 8 && LZO_SIZEOF_SIZE_T == 8)
#  define LZO_ABI_LLP64         1
#  define LZO_INFO_ABI_PM       "llp64"
#elif (LZO_SIZEOF_INT == 4 && LZO_SIZEOF_LONG == 8 && LZO_SIZEOF_VOID_P == 8)
#  define LZO_ABI_LP64          1
#  define LZO_INFO_ABI_PM       "lp64"
#elif (LZO_SIZEOF_INT == 8 && LZO_SIZEOF_LONG == 8 && LZO_SIZEOF_VOID_P == 8)
#  define LZO_ABI_ILP64         1
#  define LZO_INFO_ABI_PM       "ilp64"
#elif (LZO_SIZEOF_INT == 4 && LZO_SIZEOF_LONG == 8 && LZO_SIZEOF_VOID_P == 4)
#  define LZO_ABI_IP32L64       1
#  define LZO_INFO_ABI_PM       "ip32l64"
#endif
#if !defined(__LZO_LIBC_OVERRIDE)
#if defined(LZO_LIBC_NAKED)
#  define LZO_INFO_LIBC         "naked"
#elif defined(LZO_LIBC_FREESTANDING)
#  define LZO_INFO_LIBC         "freestanding"
#elif defined(LZO_LIBC_MOSTLY_FREESTANDING)
#  define LZO_INFO_LIBC         "mfreestanding"
#elif defined(LZO_LIBC_ISOC90)
#  define LZO_INFO_LIBC         "isoc90"
#elif defined(LZO_LIBC_ISOC99)
#  define LZO_INFO_LIBC         "isoc99"
#elif defined(__dietlibc__)
#  define LZO_LIBC_DIETLIBC     1
#  define LZO_INFO_LIBC         "dietlibc"
#elif defined(_NEWLIB_VERSION)
#  define LZO_LIBC_NEWLIB       1
#  define LZO_INFO_LIBC         "newlib"
#elif defined(__UCLIBC__) && defined(__UCLIBC_MAJOR__) && defined(__UCLIBC_MINOR__)
#  if defined(__UCLIBC_SUBLEVEL__)
#    define LZO_LIBC_UCLIBC     (__UCLIBC_MAJOR__ * 0x10000L + __UCLIBC_MINOR__ * 0x100 + __UCLIBC_SUBLEVEL__)
#  else
#    define LZO_LIBC_UCLIBC     0x00090bL
#  endif
#  define LZO_INFO_LIBC         "uclibc"
#elif defined(__GLIBC__) && defined(__GLIBC_MINOR__)
#  define LZO_LIBC_GLIBC        (__GLIBC__ * 0x10000L + __GLIBC_MINOR__ * 0x100)
#  define LZO_INFO_LIBC         "glibc"
#elif (LZO_CC_MWERKS) && defined(__MSL__)
#  define LZO_LIBC_MSL          __MSL__
#  define LZO_INFO_LIBC         "msl"
#elif 1 && defined(__IAR_SYSTEMS_ICC__)
#  define LZO_LIBC_ISOC90       1
#  define LZO_INFO_LIBC         "isoc90"
#else
#  define LZO_LIBC_DEFAULT      1
#  define LZO_INFO_LIBC         "default"
#endif
#endif
#if !defined(__lzo_gnuc_extension__)
#if (LZO_CC_GNUC >= 0x020800ul)
#  define __lzo_gnuc_extension__    __extension__
#elif (LZO_CC_LLVM || LZO_CC_PATHSCALE)
#  define __lzo_gnuc_extension__    __extension__
#else
#  define __lzo_gnuc_extension__
#endif
#endif
#if !defined(__lzo_ua_volatile)
#  define __lzo_ua_volatile     volatile
#endif
#if !defined(__lzo_alignof)
#if (LZO_CC_CILLY || LZO_CC_GNUC || LZO_CC_LLVM || LZO_CC_PATHSCALE || LZO_CC_PGI)
#  define __lzo_alignof(e)      __alignof__(e)
#elif (LZO_CC_INTELC && (__INTEL_COMPILER >= 700))
#  define __lzo_alignof(e)      __alignof__(e)
#elif (LZO_CC_MSC && (_MSC_VER >= 1300))
#  define __lzo_alignof(e)      __alignof(e)
#endif
#endif
#if defined(__lzo_alignof)
#  define __lzo_HAVE_alignof 1
#endif
#if !defined(__lzo_constructor)
#if (LZO_CC_GNUC >= 0x030400ul)
#  define __lzo_constructor     __attribute__((__constructor__,__used__))
#elif (LZO_CC_GNUC >= 0x020700ul)
#  define __lzo_constructor     __attribute__((__constructor__))
#elif (LZO_CC_LLVM || LZO_CC_PATHSCALE)
#  define __lzo_constructor     __attribute__((__constructor__))
#endif
#endif
#if defined(__lzo_constructor)
#  define __lzo_HAVE_constructor 1
#endif
#if !defined(__lzo_destructor)
#if (LZO_CC_GNUC >= 0x030400ul)
#  define __lzo_destructor      __attribute__((__destructor__,__used__))
#elif (LZO_CC_GNUC >= 0x020700ul)
#  define __lzo_destructor      __attribute__((__destructor__))
#elif (LZO_CC_LLVM || LZO_CC_PATHSCALE)
#  define __lzo_destructor      __attribute__((__destructor__))
#endif
#endif
#if defined(__lzo_destructor)
#  define __lzo_HAVE_destructor 1
#endif
#if defined(__lzo_HAVE_destructor) && !defined(__lzo_HAVE_constructor)
#  error "this should not happen"
#endif
#if !defined(__lzo_inline)
#if (LZO_CC_TURBOC && (__TURBOC__ <= 0x0295))
#elif defined(__cplusplus)
#  define __lzo_inline          inline
#elif (LZO_CC_BORLANDC && (__BORLANDC__ >= 0x0550))
#  define __lzo_inline          __inline
#elif (LZO_CC_CILLY || LZO_CC_GNUC || LZO_CC_LLVM || LZO_CC_PATHSCALE || LZO_CC_PGI)
#  define __lzo_inline          __inline__
#elif (LZO_CC_DMC)
#  define __lzo_inline          __inline
#elif (LZO_CC_INTELC)
#  define __lzo_inline          __inline
#elif (LZO_CC_MWERKS && (__MWERKS__ >= 0x2405))
#  define __lzo_inline          __inline
#elif (LZO_CC_MSC && (_MSC_VER >= 900))
#  define __lzo_inline          __inline
#elif defined(__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L)
#  define __lzo_inline          inline
#endif
#endif
#if defined(__lzo_inline)
#  define __lzo_HAVE_inline 1
#else
#  define __lzo_inline
#endif
#if !defined(__lzo_forceinline)
#if (LZO_CC_GNUC >= 0x030200ul)
#  define __lzo_forceinline     __inline__ __attribute__((__always_inline__))
#elif (LZO_CC_INTELC && (__INTEL_COMPILER >= 450) && LZO_CC_SYNTAX_MSC)
#  define __lzo_forceinline     __forceinline
#elif (LZO_CC_INTELC && (__INTEL_COMPILER >= 800) && LZO_CC_SYNTAX_GNUC)
#  define __lzo_forceinline     __inline__ __attribute__((__always_inline__))
#elif (LZO_CC_LLVM || LZO_CC_PATHSCALE)
#  define __lzo_forceinline     __inline__ __attribute__((__always_inline__))
#elif (LZO_CC_MSC && (_MSC_VER >= 1200))
#  define __lzo_forceinline     __forceinline
#endif
#endif
#if defined(__lzo_forceinline)
#  define __lzo_HAVE_forceinline 1
#else
#  define __lzo_forceinline
#endif
#if !defined(__lzo_noinline)
#if 1 && (LZO_ARCH_I386) && (LZO_CC_GNUC >= 0x040000ul) && (LZO_CC_GNUC < 0x040003ul)
#  define __lzo_noinline        __attribute__((__noinline__,__used__))
#elif (LZO_CC_GNUC >= 0x030200ul)
#  define __lzo_noinline        __attribute__((__noinline__))
#elif (LZO_CC_INTELC && (__INTEL_COMPILER >= 600) && LZO_CC_SYNTAX_MSC)
#  define __lzo_noinline        __declspec(noinline)
#elif (LZO_CC_INTELC && (__INTEL_COMPILER >= 800) && LZO_CC_SYNTAX_GNUC)
#  define __lzo_noinline        __attribute__((__noinline__))
#elif (LZO_CC_LLVM || LZO_CC_PATHSCALE)
#  define __lzo_noinline        __attribute__((__noinline__))
#elif (LZO_CC_MSC && (_MSC_VER >= 1300))
#  define __lzo_noinline        __declspec(noinline)
#elif (LZO_CC_MWERKS && (__MWERKS__ >= 0x3200) && (LZO_OS_WIN32 || LZO_OS_WIN64))
#  if defined(__cplusplus)
#  else
#    define __lzo_noinline      __declspec(noinline)
#  endif
#endif
#endif
#if defined(__lzo_noinline)
#  define __lzo_HAVE_noinline 1
#else
#  define __lzo_noinline
#endif
#if (defined(__lzo_HAVE_forceinline) || defined(__lzo_HAVE_noinline)) && !defined(__lzo_HAVE_inline)
#  error "this should not happen"
#endif
#if !defined(__lzo_noreturn)
#if (LZO_CC_GNUC >= 0x020700ul)
#  define __lzo_noreturn        __attribute__((__noreturn__))
#elif (LZO_CC_INTELC && (__INTEL_COMPILER >= 450) && LZO_CC_SYNTAX_MSC)
#  define __lzo_noreturn        __declspec(noreturn)
#elif (LZO_CC_INTELC && (__INTEL_COMPILER >= 600) && LZO_CC_SYNTAX_GNUC)
#  define __lzo_noreturn        __attribute__((__noreturn__))
#elif (LZO_CC_LLVM || LZO_CC_PATHSCALE)
#  define __lzo_noreturn        __attribute__((__noreturn__))
#elif (LZO_CC_MSC && (_MSC_VER >= 1200))
#  define __lzo_noreturn        __declspec(noreturn)
#endif
#endif
#if defined(__lzo_noreturn)
#  define __lzo_HAVE_noreturn 1
#else
#  define __lzo_noreturn
#endif
#if !defined(__lzo_nothrow)
#if (LZO_CC_GNUC >= 0x030300ul)
#  define __lzo_nothrow         __attribute__((__nothrow__))
#elif (LZO_CC_INTELC && (__INTEL_COMPILER >= 450) && LZO_CC_SYNTAX_MSC) && defined(__cplusplus)
#  define __lzo_nothrow         __declspec(nothrow)
#elif (LZO_CC_INTELC && (__INTEL_COMPILER >= 800) && LZO_CC_SYNTAX_GNUC)
#  define __lzo_nothrow         __attribute__((__nothrow__))
#elif (LZO_CC_LLVM || LZO_CC_PATHSCALE)
#  define __lzo_nothrow         __attribute__((__nothrow__))
#elif (LZO_CC_MSC && (_MSC_VER >= 1200)) && defined(__cplusplus)
#  define __lzo_nothrow         __declspec(nothrow)
#endif
#endif
#if defined(__lzo_nothrow)
#  define __lzo_HAVE_nothrow 1
#else
#  define __lzo_nothrow
#endif
#if !defined(__lzo_restrict)
#if (LZO_CC_GNUC >= 0x030400ul)
#  define __lzo_restrict        __restrict__
#elif (LZO_CC_INTELC && (__INTEL_COMPILER >= 600) && LZO_CC_SYNTAX_GNUC)
#  define __lzo_restrict        __restrict__
#elif (LZO_CC_LLVM)
#  define __lzo_restrict        __restrict__
#elif (LZO_CC_MSC && (_MSC_VER >= 1400))
#  define __lzo_restrict        __restrict
#endif
#endif
#if defined(__lzo_restrict)
#  define __lzo_HAVE_restrict 1
#else
#  define __lzo_restrict
#endif
#if !defined(__lzo_likely) && !defined(__lzo_unlikely)
#if (LZO_CC_GNUC >= 0x030200ul)
#  define __lzo_likely(e)       (__builtin_expect(!!(e),1))
#  define __lzo_unlikely(e)     (__builtin_expect(!!(e),0))
#elif (LZO_CC_INTELC && (__INTEL_COMPILER >= 800))
#  define __lzo_likely(e)       (__builtin_expect(!!(e),1))
#  define __lzo_unlikely(e)     (__builtin_expect(!!(e),0))
#elif (LZO_CC_LLVM || LZO_CC_PATHSCALE)
#  define __lzo_likely(e)       (__builtin_expect(!!(e),1))
#  define __lzo_unlikely(e)     (__builtin_expect(!!(e),0))
#endif
#endif
#if defined(__lzo_likely)
#  define __lzo_HAVE_likely 1
#else
#  define __lzo_likely(e)       (e)
#endif
#if defined(__lzo_unlikely)
#  define __lzo_HAVE_unlikely 1
#else
#  define __lzo_unlikely(e)     (e)
#endif
#if !defined(LZO_UNUSED)
#  if (LZO_CC_BORLANDC && (__BORLANDC__ >= 0x0600))
#    define LZO_UNUSED(var)         ((void) &var)
#  elif (LZO_CC_BORLANDC || LZO_CC_HIGHC || LZO_CC_NDPC || LZO_CC_PELLESC || LZO_CC_TURBOC)
#    define LZO_UNUSED(var)         if (&var) ; else
#  elif (LZO_CC_GNUC || LZO_CC_LLVM || LZO_CC_PATHSCALE)
#    define LZO_UNUSED(var)         ((void) var)
#  elif (LZO_CC_MSC && (_MSC_VER < 900))
#    define LZO_UNUSED(var)         if (&var) ; else
#  elif (LZO_CC_KEILC)
#    define LZO_UNUSED(var)         {extern int __lzo_unused[1-2*!(sizeof(var)>0)];}
#  elif (LZO_CC_PACIFICC)
#    define LZO_UNUSED(var)         ((void) sizeof(var))
#  elif (LZO_CC_WATCOMC) && defined(__cplusplus)
#    define LZO_UNUSED(var)         ((void) var)
#  else
#    define LZO_UNUSED(var)         ((void) &var)
#  endif
#endif
#if !defined(LZO_UNUSED_FUNC)
#  if (LZO_CC_BORLANDC && (__BORLANDC__ >= 0x0600))
#    define LZO_UNUSED_FUNC(func)   ((void) func)
#  elif (LZO_CC_BORLANDC || LZO_CC_NDPC || LZO_CC_TURBOC)
#    define LZO_UNUSED_FUNC(func)   if (func) ; else
#  elif (LZO_CC_LLVM)
#    define LZO_UNUSED_FUNC(func)   ((void) &func)
#  elif (LZO_CC_MSC && (_MSC_VER < 900))
#    define LZO_UNUSED_FUNC(func)   if (func) ; else
#  elif (LZO_CC_MSC)
#    define LZO_UNUSED_FUNC(func)   ((void) &func)
#  elif (LZO_CC_KEILC || LZO_CC_PELLESC)
#    define LZO_UNUSED_FUNC(func)   {extern int __lzo_unused[1-2*!(sizeof((int)func)>0)];}
#  else
#    define LZO_UNUSED_FUNC(func)   ((void) func)
#  endif
#endif
#if !defined(LZO_UNUSED_LABEL)
#  if (LZO_CC_WATCOMC) && defined(__cplusplus)
#    define LZO_UNUSED_LABEL(l)     switch(0) case 1:goto l
#  elif (LZO_CC_INTELC || LZO_CC_WATCOMC)
#    define LZO_UNUSED_LABEL(l)     if (0) goto l
#  else
#    define LZO_UNUSED_LABEL(l)     switch(0) case 1:goto l
#  endif
#endif
#if !defined(LZO_DEFINE_UNINITIALIZED_VAR)
#  if 0
#    define LZO_DEFINE_UNINITIALIZED_VAR(type,var,init)  type var
#  elif 0 && (LZO_CC_GNUC)
#    define LZO_DEFINE_UNINITIALIZED_VAR(type,var,init)  type var = var
#  else
#    define LZO_DEFINE_UNINITIALIZED_VAR(type,var,init)  type var = init
#  endif
#endif
#if !defined(LZO_COMPILE_TIME_ASSERT_HEADER)
#  if (LZO_CC_AZTECC || LZO_CC_ZORTECHC)
#    define LZO_COMPILE_TIME_ASSERT_HEADER(e)  extern int __lzo_cta[1-!(e)];
#  elif (LZO_CC_DMC || LZO_CC_SYMANTECC)
#    define LZO_COMPILE_TIME_ASSERT_HEADER(e)  extern int __lzo_cta[1u-2*!(e)];
#  elif (LZO_CC_TURBOC && (__TURBOC__ == 0x0295))
#    define LZO_COMPILE_TIME_ASSERT_HEADER(e)  extern int __lzo_cta[1-!(e)];
#  else
#    define LZO_COMPILE_TIME_ASSERT_HEADER(e)  extern int __lzo_cta[1-2*!(e)];
#  endif
#endif
#if !defined(LZO_COMPILE_TIME_ASSERT)
#  if (LZO_CC_AZTECC)
#    define LZO_COMPILE_TIME_ASSERT(e)  {typedef int __lzo_cta_t[1-!(e)];}
#  elif (LZO_CC_DMC || LZO_CC_PACIFICC || LZO_CC_SYMANTECC || LZO_CC_ZORTECHC)
#    define LZO_COMPILE_TIME_ASSERT(e)  switch(0) case 1:case !(e):break;
#  elif (LZO_CC_MSC && (_MSC_VER < 900))
#    define LZO_COMPILE_TIME_ASSERT(e)  switch(0) case 1:case !(e):break;
#  elif (LZO_CC_TURBOC && (__TURBOC__ == 0x0295))
#    define LZO_COMPILE_TIME_ASSERT(e)  switch(0) case 1:case !(e):break;
#  else
#    define LZO_COMPILE_TIME_ASSERT(e)  {typedef int __lzo_cta_t[1-2*!(e)];}
#  endif
#endif
#if (LZO_ARCH_I086 || LZO_ARCH_I386) && (LZO_OS_DOS16 || LZO_OS_DOS32 || LZO_OS_OS2 || LZO_OS_OS216 || LZO_OS_WIN16 || LZO_OS_WIN32 || LZO_OS_WIN64)
#  if (LZO_CC_GNUC || LZO_CC_HIGHC || LZO_CC_NDPC || LZO_CC_PACIFICC)
#  elif (LZO_CC_DMC || LZO_CC_SYMANTECC || LZO_CC_ZORTECHC)
#    define __lzo_cdecl                 __cdecl
#    define __lzo_cdecl_atexit
#    define __lzo_cdecl_main            __cdecl
#    if (LZO_OS_OS2 && (LZO_CC_DMC || LZO_CC_SYMANTECC))
#      define __lzo_cdecl_qsort         __pascal
#    elif (LZO_OS_OS2 && (LZO_CC_ZORTECHC))
#      define __lzo_cdecl_qsort         _stdcall
#    else
#      define __lzo_cdecl_qsort         __cdecl
#    endif
#  elif (LZO_CC_WATCOMC)
#    define __lzo_cdecl                 __cdecl
#  else
#    define __lzo_cdecl                 __cdecl
#    define __lzo_cdecl_atexit          __cdecl
#    define __lzo_cdecl_main            __cdecl
#    define __lzo_cdecl_qsort           __cdecl
#  endif
#  if (LZO_CC_GNUC || LZO_CC_HIGHC || LZO_CC_NDPC || LZO_CC_PACIFICC || LZO_CC_WATCOMC)
#  elif (LZO_OS_OS2 && (LZO_CC_DMC || LZO_CC_SYMANTECC))
#    define __lzo_cdecl_sighandler      __pascal
#  elif (LZO_OS_OS2 && (LZO_CC_ZORTECHC))
#    define __lzo_cdecl_sighandler      _stdcall
#  elif (LZO_CC_MSC && (_MSC_VER >= 1400)) && defined(_M_CEE_PURE)
#    define __lzo_cdecl_sighandler      __clrcall
#  elif (LZO_CC_MSC && (_MSC_VER >= 600 && _MSC_VER < 700))
#    if defined(_DLL)
#      define __lzo_cdecl_sighandler    _far _cdecl _loadds
#    elif defined(_MT)
#      define __lzo_cdecl_sighandler    _far _cdecl
#    else
#      define __lzo_cdecl_sighandler    _cdecl
#    endif
#  else
#    define __lzo_cdecl_sighandler      __cdecl
#  endif
#elif (LZO_ARCH_I386) && (LZO_CC_WATCOMC)
#  define __lzo_cdecl                   __cdecl
#elif (LZO_ARCH_M68K && LZO_OS_TOS && (LZO_CC_PUREC || LZO_CC_TURBOC))
#  define __lzo_cdecl                   cdecl
#endif
#if !defined(__lzo_cdecl)
#  define __lzo_cdecl
#endif
#if !defined(__lzo_cdecl_atexit)
#  define __lzo_cdecl_atexit
#endif
#if !defined(__lzo_cdecl_main)
#  define __lzo_cdecl_main
#endif
#if !defined(__lzo_cdecl_qsort)
#  define __lzo_cdecl_qsort
#endif
#if !defined(__lzo_cdecl_sighandler)
#  define __lzo_cdecl_sighandler
#endif
#if !defined(__lzo_cdecl_va)
#  define __lzo_cdecl_va                __lzo_cdecl
#endif
#if !defined(LZO_CFG_NO_WINDOWS_H)
#if (LZO_OS_CYGWIN || (LZO_OS_EMX && defined(__RSXNT__)) || LZO_OS_WIN32 || LZO_OS_WIN64)
#  if (LZO_CC_WATCOMC && (__WATCOMC__ < 1000))
#  elif (LZO_OS_WIN32 && LZO_CC_GNUC) && defined(__PW32__)
#  elif ((LZO_OS_CYGWIN || defined(__MINGW32__)) && (LZO_CC_GNUC && (LZO_CC_GNUC < 0x025f00ul)))
#  else
#    define LZO_HAVE_WINDOWS_H 1
#  endif
#endif
#endif
#if (LZO_ARCH_ALPHA)
#  define LZO_OPT_AVOID_UINT_INDEX  1
#  define LZO_OPT_AVOID_SHORT       1
#  define LZO_OPT_AVOID_USHORT      1
#elif (LZO_ARCH_AMD64)
#  define LZO_OPT_AVOID_INT_INDEX   1
#  define LZO_OPT_AVOID_UINT_INDEX  1
#  define LZO_OPT_UNALIGNED16       1
#  define LZO_OPT_UNALIGNED32       1
#  define LZO_OPT_UNALIGNED64       1
#elif (LZO_ARCH_ARM && LZO_ARCH_ARM_THUMB)
#elif (LZO_ARCH_ARM)
#  define LZO_OPT_AVOID_SHORT       1
#  define LZO_OPT_AVOID_USHORT      1
#elif (LZO_ARCH_CRIS)
#  define LZO_OPT_UNALIGNED16       1
#  define LZO_OPT_UNALIGNED32       1
#elif (LZO_ARCH_I386)
#  define LZO_OPT_UNALIGNED16       1
#  define LZO_OPT_UNALIGNED32       1
#elif (LZO_ARCH_IA64)
#  define LZO_OPT_AVOID_INT_INDEX   1
#  define LZO_OPT_AVOID_UINT_INDEX  1
#  define LZO_OPT_PREFER_POSTINC    1
#elif (LZO_ARCH_M68K)
#  define LZO_OPT_PREFER_POSTINC    1
#  define LZO_OPT_PREFER_PREDEC     1
#  if defined(__mc68020__) && !defined(__mcoldfire__)
#    define LZO_OPT_UNALIGNED16     1
#    define LZO_OPT_UNALIGNED32     1
#  endif
#elif (LZO_ARCH_MIPS)
#  define LZO_OPT_AVOID_UINT_INDEX  1
#elif (LZO_ARCH_POWERPC)
#  define LZO_OPT_PREFER_PREINC     1
#  define LZO_OPT_PREFER_PREDEC     1
#  if defined(LZO_ABI_BIG_ENDIAN)
#    define LZO_OPT_UNALIGNED16     1
#    define LZO_OPT_UNALIGNED32     1
#  endif
#elif (LZO_ARCH_S390)
#  define LZO_OPT_UNALIGNED16       1
#  define LZO_OPT_UNALIGNED32       1
#  if (LZO_SIZEOF_SIZE_T == 8)
#    define LZO_OPT_UNALIGNED64     1
#  endif
#elif (LZO_ARCH_SH)
#  define LZO_OPT_PREFER_POSTINC    1
#  define LZO_OPT_PREFER_PREDEC     1
#endif
#if !defined(LZO_CFG_NO_INLINE_ASM)
#if defined(LZO_CC_LLVM)
#  define LZO_CFG_NO_INLINE_ASM 1
#endif
#endif
#if !defined(LZO_CFG_NO_UNALIGNED)
#if defined(LZO_ABI_NEUTRAL_ENDIAN) || defined(LZO_ARCH_GENERIC)
#  define LZO_CFG_NO_UNALIGNED 1
#endif
#endif
#if defined(LZO_CFG_NO_UNALIGNED)
#  undef LZO_OPT_UNALIGNED16
#  undef LZO_OPT_UNALIGNED32
#  undef LZO_OPT_UNALIGNED64
#endif
#if defined(LZO_CFG_NO_INLINE_ASM)
#elif (LZO_ARCH_I386 && (LZO_OS_DOS32 || LZO_OS_WIN32) && (LZO_CC_DMC || LZO_CC_INTELC || LZO_CC_MSC || LZO_CC_PELLESC))
#  define LZO_ASM_SYNTAX_MSC 1
#elif (LZO_OS_WIN64 && (LZO_CC_DMC || LZO_CC_INTELC || LZO_CC_MSC || LZO_CC_PELLESC))
#elif (LZO_ARCH_I386 && (LZO_CC_GNUC || LZO_CC_INTELC || LZO_CC_PATHSCALE))
#  define LZO_ASM_SYNTAX_GNUC 1
#elif (LZO_ARCH_AMD64 && (LZO_CC_GNUC || LZO_CC_INTELC || LZO_CC_PATHSCALE))
#  define LZO_ASM_SYNTAX_GNUC 1
#endif
#if (LZO_ASM_SYNTAX_GNUC)
#if (LZO_ARCH_I386 && LZO_CC_GNUC && (LZO_CC_GNUC < 0x020000ul))
#  define __LZO_ASM_CLOBBER         "ax"
#elif (LZO_CC_INTELC)
#  define __LZO_ASM_CLOBBER         "memory"
#else
#  define __LZO_ASM_CLOBBER         "cc", "memory"
#endif
#endif
#if defined(__LZO_INFOSTR_MM)
#elif (LZO_MM_FLAT) && (defined(__LZO_INFOSTR_PM) || defined(LZO_INFO_ABI_PM))
#  define __LZO_INFOSTR_MM          ""
#elif defined(LZO_INFO_MM)
#  define __LZO_INFOSTR_MM          "." LZO_INFO_MM
#else
#  define __LZO_INFOSTR_MM          ""
#endif
#if defined(__LZO_INFOSTR_PM)
#elif defined(LZO_INFO_ABI_PM)
#  define __LZO_INFOSTR_PM          "." LZO_INFO_ABI_PM
#else
#  define __LZO_INFOSTR_PM          ""
#endif
#if defined(__LZO_INFOSTR_ENDIAN)
#elif defined(LZO_INFO_ABI_ENDIAN)
#  define __LZO_INFOSTR_ENDIAN      "." LZO_INFO_ABI_ENDIAN
#else
#  define __LZO_INFOSTR_ENDIAN      ""
#endif
#if defined(__LZO_INFOSTR_OSNAME)
#elif defined(LZO_INFO_OS_CONSOLE)
#  define __LZO_INFOSTR_OSNAME      LZO_INFO_OS "." LZO_INFO_OS_CONSOLE
#elif defined(LZO_INFO_OS_POSIX)
#  define __LZO_INFOSTR_OSNAME      LZO_INFO_OS "." LZO_INFO_OS_POSIX
#else
#  define __LZO_INFOSTR_OSNAME      LZO_INFO_OS
#endif
#if defined(__LZO_INFOSTR_LIBC)
#elif defined(LZO_INFO_LIBC)
#  define __LZO_INFOSTR_LIBC        "." LZO_INFO_LIBC
#else
#  define __LZO_INFOSTR_LIBC        ""
#endif
#if defined(__LZO_INFOSTR_CCVER)
#elif defined(LZO_INFO_CCVER)
#  define __LZO_INFOSTR_CCVER       " " LZO_INFO_CCVER
#else
#  define __LZO_INFOSTR_CCVER       ""
#endif
#define LZO_INFO_STRING \
    LZO_INFO_ARCH __LZO_INFOSTR_MM __LZO_INFOSTR_PM __LZO_INFOSTR_ENDIAN \
    " " __LZO_INFOSTR_OSNAME __LZO_INFOSTR_LIBC " " LZO_INFO_CC __LZO_INFOSTR_CCVER

#endif

#endif

#undef LZO_HAVE_CONFIG_H
#include "minilzo.h"

#if !defined(MINILZO_VERSION) || (MINILZO_VERSION != 0x2030)
#  error "version mismatch in miniLZO source files"
#endif

#ifdef MINILZO_HAVE_CONFIG_H
#  define LZO_HAVE_CONFIG_H
#endif

#ifndef __LZO_CONF_H
#define __LZO_CONF_H

#if !defined(__LZO_IN_MINILZO)
#if defined(LZO_CFG_FREESTANDING)
#  define LZO_LIBC_FREESTANDING 1
#  define LZO_OS_FREESTANDING 1
#  define ACC_LIBC_FREESTANDING 1
#  define ACC_OS_FREESTANDING 1
#endif
#if defined(LZO_CFG_NO_UNALIGNED)
#  define ACC_CFG_NO_UNALIGNED 1
#endif
#if defined(LZO_ARCH_GENERIC)
#  define ACC_ARCH_GENERIC 1
#endif
#if defined(LZO_ABI_NEUTRAL_ENDIAN)
#  define ACC_ABI_NEUTRAL_ENDIAN 1
#endif
#if defined(LZO_HAVE_CONFIG_H)
#  define ACC_CONFIG_NO_HEADER 1
#endif
#if defined(LZO_CFG_EXTRA_CONFIG_HEADER)
#  include LZO_CFG_EXTRA_CONFIG_HEADER
#endif
#if defined(__LZOCONF_H) || defined(__LZOCONF_H_INCLUDED)
#  error "include this file first"
#endif
#include "lzo/lzoconf.h"
#endif

#if (LZO_VERSION < 0x02000) || !defined(__LZOCONF_H_INCLUDED)
#  error "version mismatch"
#endif

#if (LZO_CC_BORLANDC && LZO_ARCH_I086)
#  pragma option -h
#endif

#if (LZO_CC_MSC && (_MSC_VER >= 1000))
#  pragma warning(disable: 4127 4701)
#endif
#if (LZO_CC_MSC && (_MSC_VER >= 1300))
#  pragma warning(disable: 4820)
#  pragma warning(disable: 4514 4710 4711)
#endif

#if (LZO_CC_SUNPROC)
#  pragma error_messages(off,E_END_OF_LOOP_CODE_NOT_REACHED)
#  pragma error_messages(off,E_LOOP_NOT_ENTERED_AT_TOP)
#endif

#if defined(__LZO_MMODEL_HUGE) && (!LZO_HAVE_MM_HUGE_PTR)
#  error "this should not happen - check defines for __huge"
#endif

#if defined(__LZO_IN_MINILZO) || defined(LZO_CFG_FREESTANDING)
#elif (LZO_OS_DOS16 || LZO_OS_OS216 || LZO_OS_WIN16)
#  define ACC_WANT_ACC_INCD_H 1
#  define ACC_WANT_ACC_INCE_H 1
#  define ACC_WANT_ACC_INCI_H 1
#elif 1
#  include <string.h>
#else
#  define ACC_WANT_ACC_INCD_H 1
#endif

#if (LZO_ARCH_I086)
#  define ACC_MM_AHSHIFT        LZO_MM_AHSHIFT
#  define ACC_PTR_FP_OFF(x)     (((const unsigned __far*)&(x))[0])
#  define ACC_PTR_FP_SEG(x)     (((const unsigned __far*)&(x))[1])
#  define ACC_PTR_MK_FP(s,o)    ((void __far*)(((unsigned long)(s)<<16)+(unsigned)(o)))
#endif

#if !defined(lzo_uintptr_t)
#  if defined(__LZO_MMODEL_HUGE)
#    define lzo_uintptr_t       unsigned long
#  elif 1 && defined(LZO_OS_OS400) && (LZO_SIZEOF_VOID_P == 16)
#    define __LZO_UINTPTR_T_IS_POINTER 1
     typedef char*              lzo_uintptr_t;
#    define lzo_uintptr_t       lzo_uintptr_t
#  elif (LZO_SIZEOF_SIZE_T == LZO_SIZEOF_VOID_P)
#    define lzo_uintptr_t       size_t
#  elif (LZO_SIZEOF_LONG == LZO_SIZEOF_VOID_P)
#    define lzo_uintptr_t       unsigned long
#  elif (LZO_SIZEOF_INT == LZO_SIZEOF_VOID_P)
#    define lzo_uintptr_t       unsigned int
#  elif (LZO_SIZEOF_LONG_LONG == LZO_SIZEOF_VOID_P)
#    define lzo_uintptr_t       unsigned long long
#  else
#    define lzo_uintptr_t       size_t
#  endif
#endif
LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_uintptr_t) >= sizeof(lzo_voidp))

#if 1 && !defined(LZO_CFG_FREESTANDING)
#if 1 && !defined(HAVE_STRING_H)
#define HAVE_STRING_H 1
#endif
#if 1 && !defined(HAVE_MEMCMP)
#define HAVE_MEMCMP 1
#endif
#if 1 && !defined(HAVE_MEMCPY)
#define HAVE_MEMCPY 1
#endif
#if 1 && !defined(HAVE_MEMMOVE)
#define HAVE_MEMMOVE 1
#endif
#if 1 && !defined(HAVE_MEMSET)
#define HAVE_MEMSET 1
#endif
#endif

#if 1 && defined(HAVE_STRING_H)
#include <string.h>
#endif

#if defined(LZO_CFG_FREESTANDING)
#  undef HAVE_MEMCMP
#  undef HAVE_MEMCPY
#  undef HAVE_MEMMOVE
#  undef HAVE_MEMSET
#endif

#if !defined(HAVE_MEMCMP)
#  undef memcmp
#  define memcmp(a,b,c)         lzo_memcmp(a,b,c)
#elif !defined(__LZO_MMODEL_HUGE)
#  define lzo_memcmp(a,b,c)     memcmp(a,b,c)
#endif
#if !defined(HAVE_MEMCPY)
#  undef memcpy
#  define memcpy(a,b,c)         lzo_memcpy(a,b,c)
#elif !defined(__LZO_MMODEL_HUGE)
#  define lzo_memcpy(a,b,c)     memcpy(a,b,c)
#endif
#if !defined(HAVE_MEMMOVE)
#  undef memmove
#  define memmove(a,b,c)        lzo_memmove(a,b,c)
#elif !defined(__LZO_MMODEL_HUGE)
#  define lzo_memmove(a,b,c)    memmove(a,b,c)
#endif
#if !defined(HAVE_MEMSET)
#  undef memset
#  define memset(a,b,c)         lzo_memset(a,b,c)
#elif !defined(__LZO_MMODEL_HUGE)
#  define lzo_memset(a,b,c)     memset(a,b,c)
#endif

#undef NDEBUG
#if defined(LZO_CFG_FREESTANDING)
#  undef LZO_DEBUG
#  define NDEBUG 1
#  undef assert
#  define assert(e) ((void)0)
#else
#  if !defined(LZO_DEBUG)
#    define NDEBUG 1
#  endif
#  include <assert.h>
#endif

#if 0 && defined(__BOUNDS_CHECKING_ON)
#  include <unchecked.h>
#else
#  define BOUNDS_CHECKING_OFF_DURING(stmt)      stmt
#  define BOUNDS_CHECKING_OFF_IN_EXPR(expr)     (expr)
#endif

#if !defined(__lzo_inline)
#  define __lzo_inline
#endif
#if !defined(__lzo_forceinline)
#  define __lzo_forceinline
#endif
#if !defined(__lzo_noinline)
#  define __lzo_noinline
#endif

#if 1
#  define LZO_BYTE(x)       ((unsigned char) (x))
#else
#  define LZO_BYTE(x)       ((unsigned char) ((x) & 0xff))
#endif

#define LZO_MAX(a,b)        ((a) >= (b) ? (a) : (b))
#define LZO_MIN(a,b)        ((a) <= (b) ? (a) : (b))
#define LZO_MAX3(a,b,c)     ((a) >= (b) ? LZO_MAX(a,c) : LZO_MAX(b,c))
#define LZO_MIN3(a,b,c)     ((a) <= (b) ? LZO_MIN(a,c) : LZO_MIN(b,c))

#define lzo_sizeof(type)    ((lzo_uint) (sizeof(type)))

#define LZO_HIGH(array)     ((lzo_uint) (sizeof(array)/sizeof(*(array))))

#define LZO_SIZE(bits)      (1u << (bits))
#define LZO_MASK(bits)      (LZO_SIZE(bits) - 1)

#define LZO_LSIZE(bits)     (1ul << (bits))
#define LZO_LMASK(bits)     (LZO_LSIZE(bits) - 1)

#define LZO_USIZE(bits)     ((lzo_uint) 1 << (bits))
#define LZO_UMASK(bits)     (LZO_USIZE(bits) - 1)

#if !defined(DMUL)
#if 0

#  define DMUL(a,b) ((lzo_xint) ((lzo_uint32)(a) * (lzo_uint32)(b)))
#else
#  define DMUL(a,b) ((lzo_xint) ((a) * (b)))
#endif
#endif

#if 1 && !defined(LZO_CFG_NO_UNALIGNED)
#if 1 && (LZO_ARCH_AMD64 || LZO_ARCH_I386)
#  if (LZO_SIZEOF_SHORT == 2)
#    define LZO_UNALIGNED_OK_2
#  endif
#  if (LZO_SIZEOF_INT == 4)
#    define LZO_UNALIGNED_OK_4
#  endif
#endif
#endif

#if defined(LZO_UNALIGNED_OK_2)
  LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(short) == 2)
#endif
#if defined(LZO_UNALIGNED_OK_4)
  LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_uint32) == 4)
#elif defined(LZO_ALIGNED_OK_4)
  LZO_COMPILE_TIME_ASSERT_HEADER(sizeof(lzo_uint32) == 4)
#endif

#define MEMCPY8_DS(dest,src,len) \
    lzo_memcpy(dest,src,len); dest += len; src += len

#define BZERO8_PTR(s,l,n) \
    lzo_memset((lzo_voidp)(s),0,(lzo_uint)(l)*(n))

#define MEMCPY_DS(dest,src,len) \
    do *dest++ = *src++; while (--len > 0)

__LZO_EXTERN_C int __lzo_init_done;
__LZO_EXTERN_C const char __lzo_copyright[];
LZO_EXTERN(const lzo_bytep) lzo_copyright(void);

#ifndef __LZO_PTR_H
#define __LZO_PTR_H

#ifdef __cplusplus
extern "C" {
#endif

#if !defined(lzo_uintptr_t)
#  if defined(__LZO_MMODEL_HUGE)
#    define lzo_uintptr_t   unsigned long
#  else
#    define lzo_uintptr_t   acc_uintptr_t
#    ifdef __ACC_INTPTR_T_IS_POINTER
#      define __LZO_UINTPTR_T_IS_POINTER 1
#    endif
#  endif
#endif

#if (LZO_ARCH_I086)
#define PTR(a)              ((lzo_bytep) (a))
#define PTR_ALIGNED_4(a)    ((ACC_PTR_FP_OFF(a) & 3) == 0)
#define PTR_ALIGNED2_4(a,b) (((ACC_PTR_FP_OFF(a) | ACC_PTR_FP_OFF(b)) & 3) == 0)
#elif (LZO_MM_PVP)
#define PTR(a)              ((lzo_bytep) (a))
#define PTR_ALIGNED_8(a)    ((((lzo_uintptr_t)(a)) >> 61) == 0)
#define PTR_ALIGNED2_8(a,b) ((((lzo_uintptr_t)(a)|(lzo_uintptr_t)(b)) >> 61) == 0)
#else
#define PTR(a)              ((lzo_uintptr_t) (a))
#define PTR_LINEAR(a)       PTR(a)
#define PTR_ALIGNED_4(a)    ((PTR_LINEAR(a) & 3) == 0)
#define PTR_ALIGNED_8(a)    ((PTR_LINEAR(a) & 7) == 0)
#define PTR_ALIGNED2_4(a,b) (((PTR_LINEAR(a) | PTR_LINEAR(b)) & 3) == 0)
#define PTR_ALIGNED2_8(a,b) (((PTR_LINEAR(a) | PTR_LINEAR(b)) & 7) == 0)
#endif

#define PTR_LT(a,b)         (PTR(a) < PTR(b))
#define PTR_GE(a,b)         (PTR(a) >= PTR(b))
#define PTR_DIFF(a,b)       (PTR(a) - PTR(b))
#define pd(a,b)             ((lzo_uint) ((a)-(b)))

LZO_EXTERN(lzo_uintptr_t)
__lzo_ptr_linear(const lzo_voidp ptr);

typedef union
{
    char            a_char;
    unsigned char   a_uchar;
    short           a_short;
    unsigned short  a_ushort;
    int             a_int;
    unsigned int    a_uint;
    long            a_long;
    unsigned long   a_ulong;
    lzo_int         a_lzo_int;
    lzo_uint        a_lzo_uint;
    lzo_int32       a_lzo_int32;
    lzo_uint32      a_lzo_uint32;
    ptrdiff_t       a_ptrdiff_t;
    lzo_uintptr_t   a_lzo_uintptr_t;
    lzo_voidp       a_lzo_voidp;
    void *          a_void_p;
    lzo_bytep       a_lzo_bytep;
    lzo_bytepp      a_lzo_bytepp;
    lzo_uintp       a_lzo_uintp;
    lzo_uint *      a_lzo_uint_p;
    lzo_uint32p     a_lzo_uint32p;
    lzo_uint32 *    a_lzo_uint32_p;
    unsigned char * a_uchar_p;
    char *          a_char_p;
}
lzo_full_align_t;

#ifdef __cplusplus
}
#endif

#endif

#define LZO_DETERMINISTIC

#define LZO_DICT_USE_PTR
#if 0 && (LZO_ARCH_I086)
#  undef LZO_DICT_USE_PTR
#endif

#if defined(LZO_DICT_USE_PTR)
#  define lzo_dict_t    const lzo_bytep
#  define lzo_dict_p    lzo_dict_t __LZO_MMODEL *
#else
#  define lzo_dict_t    lzo_uint
#  define lzo_dict_p    lzo_dict_t __LZO_MMODEL *
#endif

#endif

#if !defined(MINILZO_CFG_SKIP_LZO_PTR)

LZO_PUBLIC(lzo_uintptr_t)
__lzo_ptr_linear(const lzo_voidp ptr)
{
    lzo_uintptr_t p;

#if (LZO_ARCH_I086)
    p = (((lzo_uintptr_t)(ACC_PTR_FP_SEG(ptr))) << (16 - ACC_MM_AHSHIFT)) + (ACC_PTR_FP_OFF(ptr));
#elif (LZO_MM_PVP)
    p = (lzo_uintptr_t) (ptr);
    p = (p << 3) | (p >> 61);
#else
    p = (lzo_uintptr_t) PTR_LINEAR(ptr);
#endif

    return p;
}

LZO_PUBLIC(unsigned)
__lzo_align_gap(const lzo_voidp ptr, lzo_uint size)
{
#if defined(__LZO_UINTPTR_T_IS_POINTER)
    size_t n = (size_t) ptr;
    n = (((n + size - 1) / size) * size) - n;
#else
    lzo_uintptr_t p, n;
    p = __lzo_ptr_linear(ptr);
    n = (((p + size - 1) / size) * size) - p;
#endif

    assert(size > 0);
    assert((long)n >= 0);
    assert(n <= size);
    return (unsigned)n;
}

#endif

/* If you use the LZO library in a product, I would appreciate that you
 * keep this copyright string in the executable of your product.
 */

const char __lzo_copyright[] =
#if !defined(__LZO_IN_MINLZO)
    LZO_VERSION_STRING;
#else
    "\r\n\n"
    "LZO data compression library.\n"
    "$Copyright: LZO (C) 1996, 1997, 1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008 Markus Franz Xaver Johannes Oberhumer\n"
    "<markus@oberhumer.com>\n"
    "http://www.oberhumer.com $\n\n"
    "$Id: LZO version: v" LZO_VERSION_STRING ", " LZO_VERSION_DATE " $\n"
    "$Built: " __DATE__ " " __TIME__ " $\n"
    "$Info: " LZO_INFO_STRING " $\n";
#endif

LZO_PUBLIC(const lzo_bytep)
lzo_copyright(void)
{
#if (LZO_OS_DOS16 && LZO_CC_TURBOC)
    return (lzo_voidp) __lzo_copyright;
#else
    return (const lzo_bytep) __lzo_copyright;
#endif
}

LZO_PUBLIC(unsigned)
lzo_version(void)
{
    return LZO_VERSION;
}

LZO_PUBLIC(const char *)
lzo_version_string(void)
{
    return LZO_VERSION_STRING;
}

LZO_PUBLIC(const char *)
lzo_version_date(void)
{
    return LZO_VERSION_DATE;
}

LZO_PUBLIC(const lzo_charp)
_lzo_version_string(void)
{
    return LZO_VERSION_STRING;
}

LZO_PUBLIC(const lzo_charp)
_lzo_version_date(void)
{
    return LZO_VERSION_DATE;
}

#define LZO_BASE 65521u
#define LZO_NMAX 5552

#define LZO_DO1(buf,i)  s1 += buf[i]; s2 += s1
#define LZO_DO2(buf,i)  LZO_DO1(buf,i); LZO_DO1(buf,i+1);
#define LZO_DO4(buf,i)  LZO_DO2(buf,i); LZO_DO2(buf,i+2);
#define LZO_DO8(buf,i)  LZO_DO4(buf,i); LZO_DO4(buf,i+4);
#define LZO_DO16(buf,i) LZO_DO8(buf,i); LZO_DO8(buf,i+8);

LZO_PUBLIC(lzo_uint32)
lzo_adler32(lzo_uint32 adler, const lzo_bytep buf, lzo_uint len)
{
    lzo_uint32 s1 = adler & 0xffff;
    lzo_uint32 s2 = (adler >> 16) & 0xffff;
    unsigned k;

    if (buf == NULL)
        return 1;

    while (len > 0)
    {
        k = len < LZO_NMAX ? (unsigned) len : LZO_NMAX;
        len -= k;
        if (k >= 16) do
        {
            LZO_DO16(buf,0);
            buf += 16;
            k -= 16;
        } while (k >= 16);
        if (k != 0) do
        {
            s1 += *buf++;
            s2 += s1;
        } while (--k > 0);
        s1 %= LZO_BASE;
        s2 %= LZO_BASE;
    }
    return (s2 << 16) | s1;
}

#undef LZO_DO1
#undef LZO_DO2
#undef LZO_DO4
#undef LZO_DO8
#undef LZO_DO16

#if !defined(MINILZO_CFG_SKIP_LZO_STRING)
#undef lzo_memcmp
#undef lzo_memcpy
#undef lzo_memmove
#undef lzo_memset
#if !defined(__LZO_MMODEL_HUGE)
#  undef LZO_HAVE_MM_HUGE_PTR
#endif
#define lzo_hsize_t             lzo_uint
#define lzo_hvoid_p             lzo_voidp
#define lzo_hbyte_p             lzo_bytep
#define LZOLIB_PUBLIC(r,f)      LZO_PUBLIC(r) f
#define lzo_hmemcmp             lzo_memcmp
#define lzo_hmemcpy             lzo_memcpy
#define lzo_hmemmove            lzo_memmove
#define lzo_hmemset             lzo_memset
#define __LZOLIB_HMEMCPY_CH_INCLUDED 1
#if !defined(LZOLIB_PUBLIC)
#  define LZOLIB_PUBLIC(r,f)    r __LZOLIB_FUNCNAME(f)
#endif
LZOLIB_PUBLIC(int, lzo_hmemcmp) (const lzo_hvoid_p s1, const lzo_hvoid_p s2, lzo_hsize_t len)
{
#if (LZO_HAVE_MM_HUGE_PTR) || !defined(HAVE_MEMCMP)
    const lzo_hbyte_p p1 = (const lzo_hbyte_p) s1;
    const lzo_hbyte_p p2 = (const lzo_hbyte_p) s2;
    if __lzo_likely(len > 0) do
    {
        int d = *p1 - *p2;
        if (d != 0)
            return d;
        p1++; p2++;
    } while __lzo_likely(--len > 0);
    return 0;
#else
    return memcmp(s1, s2, len);
#endif
}
LZOLIB_PUBLIC(lzo_hvoid_p, lzo_hmemcpy) (lzo_hvoid_p dest, const lzo_hvoid_p src, lzo_hsize_t len)
{
#if (LZO_HAVE_MM_HUGE_PTR) || !defined(HAVE_MEMCPY)
    lzo_hbyte_p p1 = (lzo_hbyte_p) dest;
    const lzo_hbyte_p p2 = (const lzo_hbyte_p) src;
    if (!(len > 0) || p1 == p2)
        return dest;
    do
        *p1++ = *p2++;
    while __lzo_likely(--len > 0);
    return dest;
#else
    return memcpy(dest, src, len);
#endif
}
LZOLIB_PUBLIC(lzo_hvoid_p, lzo_hmemmove) (lzo_hvoid_p dest, const lzo_hvoid_p src, lzo_hsize_t len)
{
#if (LZO_HAVE_MM_HUGE_PTR) || !defined(HAVE_MEMMOVE)
    lzo_hbyte_p p1 = (lzo_hbyte_p) dest;
    const lzo_hbyte_p p2 = (const lzo_hbyte_p) src;
    if (!(len > 0) || p1 == p2)
        return dest;
    if (p1 < p2)
    {
        do
            *p1++ = *p2++;
        while __lzo_likely(--len > 0);
    }
    else
    {
        p1 += len;
        p2 += len;
        do
            *--p1 = *--p2;
        while __lzo_likely(--len > 0);
    }
    return dest;
#else
    return memmove(dest, src, len);
#endif
}
LZOLIB_PUBLIC(lzo_hvoid_p, lzo_hmemset) (lzo_hvoid_p s, int c, lzo_hsize_t len)
{
#if (LZO_HAVE_MM_HUGE_PTR) || !defined(HAVE_MEMSET)
    lzo_hbyte_p p = (lzo_hbyte_p) s;
    if __lzo_likely(len > 0) do
        *p++ = (unsigned char) c;
    while __lzo_likely(--len > 0);
    return s;
#else
    return memset(s, c, len);
#endif
}
#undef LZOLIB_PUBLIC
#endif

#if !defined(__LZO_IN_MINILZO)

#define ACC_WANT_ACC_CHK_CH 1
#undef ACCCHK_ASSERT

    ACCCHK_ASSERT_IS_SIGNED_T(lzo_int)
    ACCCHK_ASSERT_IS_UNSIGNED_T(lzo_uint)

    ACCCHK_ASSERT_IS_SIGNED_T(lzo_int32)
    ACCCHK_ASSERT_IS_UNSIGNED_T(lzo_uint32)
    ACCCHK_ASSERT((LZO_UINT32_C(1) << (int)(8*sizeof(LZO_UINT32_C(1))-1)) > 0)
    ACCCHK_ASSERT(sizeof(lzo_uint32) >= 4)

#if !defined(__LZO_UINTPTR_T_IS_POINTER)
    ACCCHK_ASSERT_IS_UNSIGNED_T(lzo_uintptr_t)
#endif
    ACCCHK_ASSERT(sizeof(lzo_uintptr_t) >= sizeof(lzo_voidp))

    ACCCHK_ASSERT_IS_UNSIGNED_T(lzo_xint)
    ACCCHK_ASSERT(sizeof(lzo_xint) >= sizeof(lzo_uint32))
    ACCCHK_ASSERT(sizeof(lzo_xint) >= sizeof(lzo_uint))
    ACCCHK_ASSERT(sizeof(lzo_xint) == sizeof(lzo_uint32) || sizeof(lzo_xint) == sizeof(lzo_uint))

#endif
#undef ACCCHK_ASSERT

LZO_PUBLIC(int)
_lzo_config_check(void)
{
    lzo_bool r = 1;
    union { unsigned char c[2*sizeof(lzo_xint)]; lzo_xint l[2]; } u;
    lzo_uintptr_t p;

#if !defined(LZO_CFG_NO_CONFIG_CHECK)
#if defined(LZO_ABI_BIG_ENDIAN)
    u.l[0] = u.l[1] = 0; u.c[sizeof(lzo_xint) - 1] = 128;
    r &= (u.l[0] == 128);
#endif
#if defined(LZO_ABI_LITTLE_ENDIAN)
    u.l[0] = u.l[1] = 0; u.c[0] = 128;
    r &= (u.l[0] == 128);
#endif
#if defined(LZO_UNALIGNED_OK_2)
    p = (lzo_uintptr_t) (const lzo_voidp) &u.c[0];
    u.l[0] = u.l[1] = 0;
    r &= ((* (const lzo_ushortp) (p+1)) == 0);
#endif
#if defined(LZO_UNALIGNED_OK_4)
    p = (lzo_uintptr_t) (const lzo_voidp) &u.c[0];
    u.l[0] = u.l[1] = 0;
    r &= ((* (const lzo_uint32p) (p+1)) == 0);
#endif
#endif

    LZO_UNUSED(u); LZO_UNUSED(p);
    return r == 1 ? LZO_E_OK : LZO_E_ERROR;
}

int __lzo_init_done = 0;

LZO_PUBLIC(int)
__lzo_init_v2(unsigned v, int s1, int s2, int s3, int s4, int s5,
                          int s6, int s7, int s8, int s9)
{
    int r;

#if defined(__LZO_IN_MINILZO)
#elif (LZO_CC_MSC && ((_MSC_VER) < 700))
#else
#define ACC_WANT_ACC_CHK_CH 1
#undef ACCCHK_ASSERT
#define ACCCHK_ASSERT(expr)  LZO_COMPILE_TIME_ASSERT(expr)
#endif
#undef ACCCHK_ASSERT

    __lzo_init_done = 1;

    if (v == 0)
        return LZO_E_ERROR;

    r = (s1 == -1 || s1 == (int) sizeof(short)) &&
        (s2 == -1 || s2 == (int) sizeof(int)) &&
        (s3 == -1 || s3 == (int) sizeof(long)) &&
        (s4 == -1 || s4 == (int) sizeof(lzo_uint32)) &&
        (s5 == -1 || s5 == (int) sizeof(lzo_uint)) &&
        (s6 == -1 || s6 == (int) lzo_sizeof_dict_t) &&
        (s7 == -1 || s7 == (int) sizeof(char *)) &&
        (s8 == -1 || s8 == (int) sizeof(lzo_voidp)) &&
        (s9 == -1 || s9 == (int) sizeof(lzo_callback_t));
    if (!r)
        return LZO_E_ERROR;

    r = _lzo_config_check();
    if (r != LZO_E_OK)
        return r;

    return r;
}

#if !defined(__LZO_IN_MINILZO)

#if (LZO_OS_WIN16 && LZO_CC_WATCOMC) && defined(__SW_BD)

#if 0
BOOL FAR PASCAL LibMain ( HANDLE hInstance, WORD wDataSegment,
                          WORD wHeapSize, LPSTR lpszCmdLine )
#else
int __far __pascal LibMain ( int a, short b, short c, long d )
#endif
{
    LZO_UNUSED(a); LZO_UNUSED(b); LZO_UNUSED(c); LZO_UNUSED(d);
    return 1;
}

#endif

#endif

#define do_compress         _lzo1x_1_do_compress

#if !defined(MINILZO_CFG_SKIP_LZO1X_1_COMPRESS)

#define LZO_NEED_DICT_H
#define D_BITS          14
#define D_INDEX1(d,p)       d = DM(DMUL(0x21,DX3(p,5,5,6)) >> 5)
#define D_INDEX2(d,p)       d = (d & (D_MASK & 0x7ff)) ^ (D_HIGH | 0x1f)

#ifndef __LZO_CONFIG1X_H
#define __LZO_CONFIG1X_H

#if !defined(LZO1X) && !defined(LZO1Y) && !defined(LZO1Z)
#  define LZO1X
#endif

#if !defined(__LZO_IN_MINILZO)
#include "lzo/lzo1x.h"
#endif

#define LZO_EOF_CODE
#undef LZO_DETERMINISTIC

#define M1_MAX_OFFSET   0x0400
#ifndef M2_MAX_OFFSET
#define M2_MAX_OFFSET   0x0800
#endif
#define M3_MAX_OFFSET   0x4000
#define M4_MAX_OFFSET   0xbfff

#define MX_MAX_OFFSET   (M1_MAX_OFFSET + M2_MAX_OFFSET)

#define M1_MIN_LEN      2
#define M1_MAX_LEN      2
#define M2_MIN_LEN      3
#ifndef M2_MAX_LEN
#define M2_MAX_LEN      8
#endif
#define M3_MIN_LEN      3
#define M3_MAX_LEN      33
#define M4_MIN_LEN      3
#define M4_MAX_LEN      9

#define M1_MARKER       0
#define M2_MARKER       64
#define M3_MARKER       32
#define M4_MARKER       16

#ifndef MIN_LOOKAHEAD
#define MIN_LOOKAHEAD       (M2_MAX_LEN + 1)
#endif

#if defined(LZO_NEED_DICT_H)

#ifndef LZO_HASH
#define LZO_HASH            LZO_HASH_LZO_INCREMENTAL_B
#endif
#define DL_MIN_LEN          M2_MIN_LEN

#ifndef __LZO_DICT_H
#define __LZO_DICT_H

#ifdef __cplusplus
extern "C" {
#endif

#if !defined(D_BITS) && defined(DBITS)
#  define D_BITS        DBITS
#endif
#if !defined(D_BITS)
#  error "D_BITS is not defined"
#endif
#if (D_BITS < 16)
#  define D_SIZE        LZO_SIZE(D_BITS)
#  define D_MASK        LZO_MASK(D_BITS)
#else
#  define D_SIZE        LZO_USIZE(D_BITS)
#  define D_MASK        LZO_UMASK(D_BITS)
#endif
#define D_HIGH          ((D_MASK >> 1) + 1)

#if !defined(DD_BITS)
#  define DD_BITS       0
#endif
#define DD_SIZE         LZO_SIZE(DD_BITS)
#define DD_MASK         LZO_MASK(DD_BITS)

#if !defined(DL_BITS)
#  define DL_BITS       (D_BITS - DD_BITS)
#endif
#if (DL_BITS < 16)
#  define DL_SIZE       LZO_SIZE(DL_BITS)
#  define DL_MASK       LZO_MASK(DL_BITS)
#else
#  define DL_SIZE       LZO_USIZE(DL_BITS)
#  define DL_MASK       LZO_UMASK(DL_BITS)
#endif

#if (D_BITS != DL_BITS + DD_BITS)
#  error "D_BITS does not match"
#endif
#if (D_BITS < 8 || D_BITS > 18)
#  error "invalid D_BITS"
#endif
#if (DL_BITS < 8 || DL_BITS > 20)
#  error "invalid DL_BITS"
#endif
#if (DD_BITS < 0 || DD_BITS > 6)
#  error "invalid DD_BITS"
#endif

#if !defined(DL_MIN_LEN)
#  define DL_MIN_LEN    3
#endif
#if !defined(DL_SHIFT)
#  define DL_SHIFT      ((DL_BITS + (DL_MIN_LEN - 1)) / DL_MIN_LEN)
#endif

#define LZO_HASH_GZIP                   1
#define LZO_HASH_GZIP_INCREMENTAL       2
#define LZO_HASH_LZO_INCREMENTAL_A      3
#define LZO_HASH_LZO_INCREMENTAL_B      4

#if !defined(LZO_HASH)
#  error "choose a hashing strategy"
#endif

#undef DM
#undef DX

#if (DL_MIN_LEN == 3)
#  define _DV2_A(p,shift1,shift2) \
        (((( (lzo_xint)((p)[0]) << shift1) ^ (p)[1]) << shift2) ^ (p)[2])
#  define _DV2_B(p,shift1,shift2) \
        (((( (lzo_xint)((p)[2]) << shift1) ^ (p)[1]) << shift2) ^ (p)[0])
#  define _DV3_B(p,shift1,shift2,shift3) \
        ((_DV2_B((p)+1,shift1,shift2) << (shift3)) ^ (p)[0])
#elif (DL_MIN_LEN == 2)
#  define _DV2_A(p,shift1,shift2) \
        (( (lzo_xint)(p[0]) << shift1) ^ p[1])
#  define _DV2_B(p,shift1,shift2) \
        (( (lzo_xint)(p[1]) << shift1) ^ p[2])
#else
#  error "invalid DL_MIN_LEN"
#endif
#define _DV_A(p,shift)      _DV2_A(p,shift,shift)
#define _DV_B(p,shift)      _DV2_B(p,shift,shift)
#define DA2(p,s1,s2) \
        (((((lzo_xint)((p)[2]) << (s2)) + (p)[1]) << (s1)) + (p)[0])
#define DS2(p,s1,s2) \
        (((((lzo_xint)((p)[2]) << (s2)) - (p)[1]) << (s1)) - (p)[0])
#define DX2(p,s1,s2) \
        (((((lzo_xint)((p)[2]) << (s2)) ^ (p)[1]) << (s1)) ^ (p)[0])
#define DA3(p,s1,s2,s3) ((DA2((p)+1,s2,s3) << (s1)) + (p)[0])
#define DS3(p,s1,s2,s3) ((DS2((p)+1,s2,s3) << (s1)) - (p)[0])
#define DX3(p,s1,s2,s3) ((DX2((p)+1,s2,s3) << (s1)) ^ (p)[0])
#define DMS(v,s)        ((lzo_uint) (((v) & (D_MASK >> (s))) << (s)))
#define DM(v)           DMS(v,0)

#if (LZO_HASH == LZO_HASH_GZIP)
#  define _DINDEX(dv,p)     (_DV_A((p),DL_SHIFT))

#elif (LZO_HASH == LZO_HASH_GZIP_INCREMENTAL)
#  define __LZO_HASH_INCREMENTAL
#  define DVAL_FIRST(dv,p)  dv = _DV_A((p),DL_SHIFT)
#  define DVAL_NEXT(dv,p)   dv = (((dv) << DL_SHIFT) ^ p[2])
#  define _DINDEX(dv,p)     (dv)
#  define DVAL_LOOKAHEAD    DL_MIN_LEN

#elif (LZO_HASH == LZO_HASH_LZO_INCREMENTAL_A)
#  define __LZO_HASH_INCREMENTAL
#  define DVAL_FIRST(dv,p)  dv = _DV_A((p),5)
#  define DVAL_NEXT(dv,p) \
                dv ^= (lzo_xint)(p[-1]) << (2*5); dv = (((dv) << 5) ^ p[2])
#  define _DINDEX(dv,p)     ((DMUL(0x9f5f,dv)) >> 5)
#  define DVAL_LOOKAHEAD    DL_MIN_LEN

#elif (LZO_HASH == LZO_HASH_LZO_INCREMENTAL_B)
#  define __LZO_HASH_INCREMENTAL
#  define DVAL_FIRST(dv,p)  dv = _DV_B((p),5)
#  define DVAL_NEXT(dv,p) \
                dv ^= p[-1]; dv = (((dv) >> 5) ^ ((lzo_xint)(p[2]) << (2*5)))
#  define _DINDEX(dv,p)     ((DMUL(0x9f5f,dv)) >> 5)
#  define DVAL_LOOKAHEAD    DL_MIN_LEN

#else
#  error "choose a hashing strategy"
#endif

#ifndef DINDEX
#define DINDEX(dv,p)        ((lzo_uint)((_DINDEX(dv,p)) & DL_MASK) << DD_BITS)
#endif
#if !defined(DINDEX1) && defined(D_INDEX1)
#define DINDEX1             D_INDEX1
#endif
#if !defined(DINDEX2) && defined(D_INDEX2)
#define DINDEX2             D_INDEX2
#endif

#if !defined(__LZO_HASH_INCREMENTAL)
#  define DVAL_FIRST(dv,p)  ((void) 0)
#  define DVAL_NEXT(dv,p)   ((void) 0)
#  define DVAL_LOOKAHEAD    0
#endif

#if !defined(DVAL_ASSERT)
#if defined(__LZO_HASH_INCREMENTAL) && !defined(NDEBUG)
static void DVAL_ASSERT(lzo_xint dv, const lzo_bytep p)
{
    lzo_xint df;
    DVAL_FIRST(df,(p));
    assert(DINDEX(dv,p) == DINDEX(df,p));
}
#else
#  define DVAL_ASSERT(dv,p) ((void) 0)
#endif
#endif

#if defined(LZO_DICT_USE_PTR)
#  define DENTRY(p,in)                          (p)
#  define GINDEX(m_pos,m_off,dict,dindex,in)    m_pos = dict[dindex]
#else
#  define DENTRY(p,in)                          ((lzo_uint) ((p)-(in)))
#  define GINDEX(m_pos,m_off,dict,dindex,in)    m_off = dict[dindex]
#endif

#if (DD_BITS == 0)

#  define UPDATE_D(dict,drun,dv,p,in)       dict[ DINDEX(dv,p) ] = DENTRY(p,in)
#  define UPDATE_I(dict,drun,index,p,in)    dict[index] = DENTRY(p,in)
#  define UPDATE_P(ptr,drun,p,in)           (ptr)[0] = DENTRY(p,in)

#else

#  define UPDATE_D(dict,drun,dv,p,in)   \
        dict[ DINDEX(dv,p) + drun++ ] = DENTRY(p,in); drun &= DD_MASK
#  define UPDATE_I(dict,drun,index,p,in)    \
        dict[ (index) + drun++ ] = DENTRY(p,in); drun &= DD_MASK
#  define UPDATE_P(ptr,drun,p,in)   \
        (ptr) [ drun++ ] = DENTRY(p,in); drun &= DD_MASK

#endif

#if defined(LZO_DICT_USE_PTR)

#define LZO_CHECK_MPOS_DET(m_pos,m_off,in,ip,max_offset) \
        (m_pos == NULL || (m_off = pd(ip, m_pos)) > max_offset)

#define LZO_CHECK_MPOS_NON_DET(m_pos,m_off,in,ip,max_offset) \
    (BOUNDS_CHECKING_OFF_IN_EXPR(( \
        m_pos = ip - (lzo_uint) PTR_DIFF(ip,m_pos), \
        PTR_LT(m_pos,in) || \
        (m_off = (lzo_uint) PTR_DIFF(ip,m_pos)) <= 0 || \
         m_off > max_offset )))

#else

#define LZO_CHECK_MPOS_DET(m_pos,m_off,in,ip,max_offset) \
        (m_off == 0 || \
         ((m_off = pd(ip, in) - m_off) > max_offset) || \
         (m_pos = (ip) - (m_off), 0) )

#define LZO_CHECK_MPOS_NON_DET(m_pos,m_off,in,ip,max_offset) \
        (pd(ip, in) <= m_off || \
         ((m_off = pd(ip, in) - m_off) > max_offset) || \
         (m_pos = (ip) - (m_off), 0) )

#endif

#if defined(LZO_DETERMINISTIC)
#  define LZO_CHECK_MPOS    LZO_CHECK_MPOS_DET
#else
#  define LZO_CHECK_MPOS    LZO_CHECK_MPOS_NON_DET
#endif

#ifdef __cplusplus
}
#endif

#endif

#endif

#endif

#define DO_COMPRESS     lzo1x_1_compress

static __lzo_noinline lzo_uint
do_compress ( const lzo_bytep in , lzo_uint  in_len,
                    lzo_bytep out, lzo_uintp out_len,
                    lzo_voidp wrkmem )
{
    register const lzo_bytep ip;
    lzo_bytep op;
    const lzo_bytep const in_end = in + in_len;
    const lzo_bytep const ip_end = in + in_len - M2_MAX_LEN - 5;
    const lzo_bytep ii;
    lzo_dict_p const dict = (lzo_dict_p) wrkmem;

    op = out;
    ip = in;
    ii = ip;

    ip += 4;
    for (;;)
    {
        register const lzo_bytep m_pos;
        lzo_uint m_off;
        lzo_uint m_len;
        lzo_uint dindex;

        DINDEX1(dindex,ip);
        GINDEX(m_pos,m_off,dict,dindex,in);
        if (LZO_CHECK_MPOS_NON_DET(m_pos,m_off,in,ip,M4_MAX_OFFSET))
            goto literal;
#if 1
        if (m_off <= M2_MAX_OFFSET || m_pos[3] == ip[3])
            goto try_match;
        DINDEX2(dindex,ip);
#endif
        GINDEX(m_pos,m_off,dict,dindex,in);
        if (LZO_CHECK_MPOS_NON_DET(m_pos,m_off,in,ip,M4_MAX_OFFSET))
            goto literal;
        if (m_off <= M2_MAX_OFFSET || m_pos[3] == ip[3])
            goto try_match;
        goto literal;

try_match:
#if 1 && defined(LZO_UNALIGNED_OK_2)
        if (* (const lzo_ushortp) m_pos != * (const lzo_ushortp) ip)
#else
        if (m_pos[0] != ip[0] || m_pos[1] != ip[1])
#endif
        {
        }
        else
        {
            if __lzo_likely(m_pos[2] == ip[2])
            {
#if 0
                if (m_off <= M2_MAX_OFFSET)
                    goto match;
                if (lit <= 3)
                    goto match;
                if (lit == 3)
                {
                    assert(op - 2 > out); op[-2] |= LZO_BYTE(3);
                    *op++ = *ii++; *op++ = *ii++; *op++ = *ii++;
                    goto code_match;
                }
                if (m_pos[3] == ip[3])
#endif
                    goto match;
            }
            else
            {
#if 0
#if 0
                if (m_off <= M1_MAX_OFFSET && lit > 0 && lit <= 3)
#else
                if (m_off <= M1_MAX_OFFSET && lit == 3)
#endif
                {
                    register lzo_uint t;

                    t = lit;
                    assert(op - 2 > out); op[-2] |= LZO_BYTE(t);
                    do *op++ = *ii++; while (--t > 0);
                    assert(ii == ip);
                    m_off -= 1;
                    *op++ = LZO_BYTE(M1_MARKER | ((m_off & 3) << 2));
                    *op++ = LZO_BYTE(m_off >> 2);
                    ip += 2;
                    goto match_done;
                }
#endif
            }
        }

literal:
        UPDATE_I(dict,0,dindex,ip,in);
        ++ip;
        if __lzo_unlikely(ip >= ip_end)
            break;
        continue;

match:
        UPDATE_I(dict,0,dindex,ip,in);
        if (pd(ip,ii) > 0)
        {
            register lzo_uint t = pd(ip,ii);

            if (t <= 3)
            {
                assert(op - 2 > out);
                op[-2] |= LZO_BYTE(t);
            }
            else if (t <= 18)
                *op++ = LZO_BYTE(t - 3);
            else
            {
                register lzo_uint tt = t - 18;

                *op++ = 0;
                while (tt > 255)
                {
                    tt -= 255;
                    *op++ = 0;
                }
                assert(tt > 0);
                *op++ = LZO_BYTE(tt);
            }
            do *op++ = *ii++; while (--t > 0);
        }

        assert(ii == ip);
        ip += 3;
        if (m_pos[3] != *ip++ || m_pos[4] != *ip++ || m_pos[5] != *ip++ ||
            m_pos[6] != *ip++ || m_pos[7] != *ip++ || m_pos[8] != *ip++
#ifdef LZO1Y
            || m_pos[ 9] != *ip++ || m_pos[10] != *ip++ || m_pos[11] != *ip++
            || m_pos[12] != *ip++ || m_pos[13] != *ip++ || m_pos[14] != *ip++
#endif
           )
        {
            --ip;
            m_len = pd(ip, ii);
            assert(m_len >= 3); assert(m_len <= M2_MAX_LEN);

            if (m_off <= M2_MAX_OFFSET)
            {
                m_off -= 1;
#if defined(LZO1X)
                *op++ = LZO_BYTE(((m_len - 1) << 5) | ((m_off & 7) << 2));
                *op++ = LZO_BYTE(m_off >> 3);
#elif defined(LZO1Y)
                *op++ = LZO_BYTE(((m_len + 1) << 4) | ((m_off & 3) << 2));
                *op++ = LZO_BYTE(m_off >> 2);
#endif
            }
            else if (m_off <= M3_MAX_OFFSET)
            {
                m_off -= 1;
                *op++ = LZO_BYTE(M3_MARKER | (m_len - 2));
                goto m3_m4_offset;
            }
            else
#if defined(LZO1X)
            {
                m_off -= 0x4000;
                assert(m_off > 0); assert(m_off <= 0x7fff);
                *op++ = LZO_BYTE(M4_MARKER |
                                 ((m_off & 0x4000) >> 11) | (m_len - 2));
                goto m3_m4_offset;
            }
#elif defined(LZO1Y)
                goto m4_match;
#endif
        }
        else
        {
            {
                const lzo_bytep end = in_end;
                const lzo_bytep m = m_pos + M2_MAX_LEN + 1;
                while (ip < end && *m == *ip)
                    m++, ip++;
                m_len = pd(ip, ii);
            }
            assert(m_len > M2_MAX_LEN);

            if (m_off <= M3_MAX_OFFSET)
            {
                m_off -= 1;
                if (m_len <= 33)
                    *op++ = LZO_BYTE(M3_MARKER | (m_len - 2));
                else
                {
                    m_len -= 33;
                    *op++ = M3_MARKER | 0;
                    goto m3_m4_len;
                }
            }
            else
            {
#if defined(LZO1Y)
m4_match:
#endif
                m_off -= 0x4000;
                assert(m_off > 0); assert(m_off <= 0x7fff);
                if (m_len <= M4_MAX_LEN)
                    *op++ = LZO_BYTE(M4_MARKER |
                                     ((m_off & 0x4000) >> 11) | (m_len - 2));
                else
                {
                    m_len -= M4_MAX_LEN;
                    *op++ = LZO_BYTE(M4_MARKER | ((m_off & 0x4000) >> 11));
m3_m4_len:
                    while (m_len > 255)
                    {
                        m_len -= 255;
                        *op++ = 0;
                    }
                    assert(m_len > 0);
                    *op++ = LZO_BYTE(m_len);
                }
            }

m3_m4_offset:
            *op++ = LZO_BYTE((m_off & 63) << 2);
            *op++ = LZO_BYTE(m_off >> 6);
        }

#if 0
match_done:
#endif
        ii = ip;
        if __lzo_unlikely(ip >= ip_end)
            break;
    }

    *out_len = pd(op, out);
    return pd(in_end,ii);
}

LZO_PUBLIC(int)
DO_COMPRESS      ( const lzo_bytep in , lzo_uint  in_len,
                         lzo_bytep out, lzo_uintp out_len,
                         lzo_voidp wrkmem )
{
    lzo_bytep op = out;
    lzo_uint t;

    if __lzo_unlikely(in_len <= M2_MAX_LEN + 5)
        t = in_len;
    else
    {
        t = do_compress(in,in_len,op,out_len,wrkmem);
        op += *out_len;
    }

    if (t > 0)
    {
        const lzo_bytep ii = in + in_len - t;

        if (op == out && t <= 238)
            *op++ = LZO_BYTE(17 + t);
        else if (t <= 3)
            op[-2] |= LZO_BYTE(t);
        else if (t <= 18)
            *op++ = LZO_BYTE(t - 3);
        else
        {
            lzo_uint tt = t - 18;

            *op++ = 0;
            while (tt > 255)
            {
                tt -= 255;
                *op++ = 0;
            }
            assert(tt > 0);
            *op++ = LZO_BYTE(tt);
        }
        do *op++ = *ii++; while (--t > 0);
    }

    *op++ = M4_MARKER | 1;
    *op++ = 0;
    *op++ = 0;

    *out_len = pd(op, out);
    return LZO_E_OK;
}

#endif

#undef do_compress
#undef DO_COMPRESS
#undef LZO_HASH

#undef LZO_TEST_OVERRUN
#undef DO_DECOMPRESS
#define DO_DECOMPRESS       lzo1x_decompress

#if !defined(MINILZO_CFG_SKIP_LZO1X_DECOMPRESS)

#if defined(LZO_TEST_OVERRUN)
#  if !defined(LZO_TEST_OVERRUN_INPUT)
#    define LZO_TEST_OVERRUN_INPUT       2
#  endif
#  if !defined(LZO_TEST_OVERRUN_OUTPUT)
#    define LZO_TEST_OVERRUN_OUTPUT      2
#  endif
#  if !defined(LZO_TEST_OVERRUN_LOOKBEHIND)
#    define LZO_TEST_OVERRUN_LOOKBEHIND
#  endif
#endif

#undef TEST_IP
#undef TEST_OP
#undef TEST_LB
#undef TEST_LBO
#undef NEED_IP
#undef NEED_OP
#undef HAVE_TEST_IP
#undef HAVE_TEST_OP
#undef HAVE_NEED_IP
#undef HAVE_NEED_OP
#undef HAVE_ANY_IP
#undef HAVE_ANY_OP

#if defined(LZO_TEST_OVERRUN_INPUT)
#  if (LZO_TEST_OVERRUN_INPUT >= 1)
#    define TEST_IP             (ip < ip_end)
#  endif
#  if (LZO_TEST_OVERRUN_INPUT >= 2)
#    define NEED_IP(x) \
            if ((lzo_uint)(ip_end - ip) < (lzo_uint)(x))  goto input_overrun
#  endif
#endif

#if defined(LZO_TEST_OVERRUN_OUTPUT)
#  if (LZO_TEST_OVERRUN_OUTPUT >= 1)
#    define TEST_OP             (op <= op_end)
#  endif
#  if (LZO_TEST_OVERRUN_OUTPUT >= 2)
#    undef TEST_OP
#    define NEED_OP(x) \
            if ((lzo_uint)(op_end - op) < (lzo_uint)(x))  goto output_overrun
#  endif
#endif

#if defined(LZO_TEST_OVERRUN_LOOKBEHIND)
#  define TEST_LB(m_pos)        if (m_pos < out || m_pos >= op) goto lookbehind_overrun
#  define TEST_LBO(m_pos,o)     if (m_pos < out || m_pos >= op - (o)) goto lookbehind_overrun
#else
#  define TEST_LB(m_pos)        ((void) 0)
#  define TEST_LBO(m_pos,o)     ((void) 0)
#endif

#if !defined(LZO_EOF_CODE) && !defined(TEST_IP)
#  define TEST_IP               (ip < ip_end)
#endif

#if defined(TEST_IP)
#  define HAVE_TEST_IP
#else
#  define TEST_IP               1
#endif
#if defined(TEST_OP)
#  define HAVE_TEST_OP
#else
#  define TEST_OP               1
#endif

#if defined(NEED_IP)
#  define HAVE_NEED_IP
#else
#  define NEED_IP(x)            ((void) 0)
#endif
#if defined(NEED_OP)
#  define HAVE_NEED_OP
#else
#  define NEED_OP(x)            ((void) 0)
#endif

#if defined(HAVE_TEST_IP) || defined(HAVE_NEED_IP)
#  define HAVE_ANY_IP
#endif
#if defined(HAVE_TEST_OP) || defined(HAVE_NEED_OP)
#  define HAVE_ANY_OP
#endif

#undef __COPY4
#define __COPY4(dst,src)    * (lzo_uint32p)(dst) = * (const lzo_uint32p)(src)

#undef COPY4
#if defined(LZO_UNALIGNED_OK_4)
#  define COPY4(dst,src)    __COPY4(dst,src)
#elif defined(LZO_ALIGNED_OK_4)
#  define COPY4(dst,src)    __COPY4((lzo_uintptr_t)(dst),(lzo_uintptr_t)(src))
#endif

#if defined(DO_DECOMPRESS)
LZO_PUBLIC(int)
DO_DECOMPRESS  ( const lzo_bytep in , lzo_uint  in_len,
                       lzo_bytep out, lzo_uintp out_len,
                       lzo_voidp wrkmem )
#endif
{
    register lzo_bytep op;
    register const lzo_bytep ip;
    register lzo_uint t;
#if defined(COPY_DICT)
    lzo_uint m_off;
    const lzo_bytep dict_end;
#else
    register const lzo_bytep m_pos;
#endif

    const lzo_bytep const ip_end = in + in_len;
#if defined(HAVE_ANY_OP)
    lzo_bytep const op_end = out + *out_len;
#endif
#if defined(LZO1Z)
    lzo_uint last_m_off = 0;
#endif

    LZO_UNUSED(wrkmem);

#if defined(COPY_DICT)
    if (dict)
    {
        if (dict_len > M4_MAX_OFFSET)
        {
            dict += dict_len - M4_MAX_OFFSET;
            dict_len = M4_MAX_OFFSET;
        }
        dict_end = dict + dict_len;
    }
    else
    {
        dict_len = 0;
        dict_end = NULL;
    }
#endif

    *out_len = 0;

    op = out;
    ip = in;

    if (*ip > 17)
    {
        t = *ip++ - 17;
        if (t < 4)
            goto match_next;
        assert(t > 0); NEED_OP(t); NEED_IP(t+1);
        do *op++ = *ip++; while (--t > 0);
        goto first_literal_run;
    }

    while (TEST_IP && TEST_OP)
    {
        t = *ip++;
        if (t >= 16)
            goto match;
        if (t == 0)
        {
            NEED_IP(1);
            while (*ip == 0)
            {
                t += 255;
                ip++;
                NEED_IP(1);
            }
            t += 15 + *ip++;
        }
        assert(t > 0); NEED_OP(t+3); NEED_IP(t+4);
#if defined(LZO_UNALIGNED_OK_4) || defined(LZO_ALIGNED_OK_4)
#if !defined(LZO_UNALIGNED_OK_4)
        if (PTR_ALIGNED2_4(op,ip))
        {
#endif
        COPY4(op,ip);
        op += 4; ip += 4;
        if (--t > 0)
        {
            if (t >= 4)
            {
                do {
                    COPY4(op,ip);
                    op += 4; ip += 4; t -= 4;
                } while (t >= 4);
                if (t > 0) do *op++ = *ip++; while (--t > 0);
            }
            else
                do *op++ = *ip++; while (--t > 0);
        }
#if !defined(LZO_UNALIGNED_OK_4)
        }
        else
#endif
#endif
#if !defined(LZO_UNALIGNED_OK_4)
        {
            *op++ = *ip++; *op++ = *ip++; *op++ = *ip++;
            do *op++ = *ip++; while (--t > 0);
        }
#endif

first_literal_run:

        t = *ip++;
        if (t >= 16)
            goto match;
#if defined(COPY_DICT)
#if defined(LZO1Z)
        m_off = (1 + M2_MAX_OFFSET) + (t << 6) + (*ip++ >> 2);
        last_m_off = m_off;
#else
        m_off = (1 + M2_MAX_OFFSET) + (t >> 2) + (*ip++ << 2);
#endif
        NEED_OP(3);
        t = 3; COPY_DICT(t,m_off)
#else
#if defined(LZO1Z)
        t = (1 + M2_MAX_OFFSET) + (t << 6) + (*ip++ >> 2);
        m_pos = op - t;
        last_m_off = t;
#else
        m_pos = op - (1 + M2_MAX_OFFSET);
        m_pos -= t >> 2;
        m_pos -= *ip++ << 2;
#endif
        TEST_LB(m_pos); NEED_OP(3);
        *op++ = *m_pos++; *op++ = *m_pos++; *op++ = *m_pos;
#endif
        goto match_done;

        do {
match:
            if (t >= 64)
            {
#if defined(COPY_DICT)
#if defined(LZO1X)
                m_off = 1 + ((t >> 2) & 7) + (*ip++ << 3);
                t = (t >> 5) - 1;
#elif defined(LZO1Y)
                m_off = 1 + ((t >> 2) & 3) + (*ip++ << 2);
                t = (t >> 4) - 3;
#elif defined(LZO1Z)
                m_off = t & 0x1f;
                if (m_off >= 0x1c)
                    m_off = last_m_off;
                else
                {
                    m_off = 1 + (m_off << 6) + (*ip++ >> 2);
                    last_m_off = m_off;
                }
                t = (t >> 5) - 1;
#endif
#else
#if defined(LZO1X)
                m_pos = op - 1;
                m_pos -= (t >> 2) & 7;
                m_pos -= *ip++ << 3;
                t = (t >> 5) - 1;
#elif defined(LZO1Y)
                m_pos = op - 1;
                m_pos -= (t >> 2) & 3;
                m_pos -= *ip++ << 2;
                t = (t >> 4) - 3;
#elif defined(LZO1Z)
                {
                    lzo_uint off = t & 0x1f;
                    m_pos = op;
                    if (off >= 0x1c)
                    {
                        assert(last_m_off > 0);
                        m_pos -= last_m_off;
                    }
                    else
                    {
                        off = 1 + (off << 6) + (*ip++ >> 2);
                        m_pos -= off;
                        last_m_off = off;
                    }
                }
                t = (t >> 5) - 1;
#endif
                TEST_LB(m_pos); assert(t > 0); NEED_OP(t+3-1);
                goto copy_match;
#endif
            }
            else if (t >= 32)
            {
                t &= 31;
                if (t == 0)
                {
                    NEED_IP(1);
                    while (*ip == 0)
                    {
                        t += 255;
                        ip++;
                        NEED_IP(1);
                    }
                    t += 31 + *ip++;
                }
#if defined(COPY_DICT)
#if defined(LZO1Z)
                m_off = 1 + (ip[0] << 6) + (ip[1] >> 2);
                last_m_off = m_off;
#else
                m_off = 1 + (ip[0] >> 2) + (ip[1] << 6);
#endif
#else
#if defined(LZO1Z)
                {
                    lzo_uint off = 1 + (ip[0] << 6) + (ip[1] >> 2);
                    m_pos = op - off;
                    last_m_off = off;
                }
#elif defined(LZO_UNALIGNED_OK_2) && defined(LZO_ABI_LITTLE_ENDIAN)
                m_pos = op - 1;
                m_pos -= (* (const lzo_ushortp) ip) >> 2;
#else
                m_pos = op - 1;
                m_pos -= (ip[0] >> 2) + (ip[1] << 6);
#endif
#endif
                ip += 2;
            }
            else if (t >= 16)
            {
#if defined(COPY_DICT)
                m_off = (t & 8) << 11;
#else
                m_pos = op;
                m_pos -= (t & 8) << 11;
#endif
                t &= 7;
                if (t == 0)
                {
                    NEED_IP(1);
                    while (*ip == 0)
                    {
                        t += 255;
                        ip++;
                        NEED_IP(1);
                    }
                    t += 7 + *ip++;
                }
#if defined(COPY_DICT)
#if defined(LZO1Z)
                m_off += (ip[0] << 6) + (ip[1] >> 2);
#else
                m_off += (ip[0] >> 2) + (ip[1] << 6);
#endif
                ip += 2;
                if (m_off == 0)
                    goto eof_found;
                m_off += 0x4000;
#if defined(LZO1Z)
                last_m_off = m_off;
#endif
#else
#if defined(LZO1Z)
                m_pos -= (ip[0] << 6) + (ip[1] >> 2);
#elif defined(LZO_UNALIGNED_OK_2) && defined(LZO_ABI_LITTLE_ENDIAN)
                m_pos -= (* (const lzo_ushortp) ip) >> 2;
#else
                m_pos -= (ip[0] >> 2) + (ip[1] << 6);
#endif
                ip += 2;
                if (m_pos == op)
                    goto eof_found;
                m_pos -= 0x4000;
#if defined(LZO1Z)
                last_m_off = pd((const lzo_bytep)op, m_pos);
#endif
#endif
            }
            else
            {
#if defined(COPY_DICT)
#if defined(LZO1Z)
                m_off = 1 + (t << 6) + (*ip++ >> 2);
                last_m_off = m_off;
#else
                m_off = 1 + (t >> 2) + (*ip++ << 2);
#endif
                NEED_OP(2);
                t = 2; COPY_DICT(t,m_off)
#else
#if defined(LZO1Z)
                t = 1 + (t << 6) + (*ip++ >> 2);
                m_pos = op - t;
                last_m_off = t;
#else
                m_pos = op - 1;
                m_pos -= t >> 2;
                m_pos -= *ip++ << 2;
#endif
                TEST_LB(m_pos); NEED_OP(2);
                *op++ = *m_pos++; *op++ = *m_pos;
#endif
                goto match_done;
            }

#if defined(COPY_DICT)

            NEED_OP(t+3-1);
            t += 3-1; COPY_DICT(t,m_off)

#else

            TEST_LB(m_pos); assert(t > 0); NEED_OP(t+3-1);
#if defined(LZO_UNALIGNED_OK_4) || defined(LZO_ALIGNED_OK_4)
#if !defined(LZO_UNALIGNED_OK_4)
            if (t >= 2 * 4 - (3 - 1) && PTR_ALIGNED2_4(op,m_pos))
            {
                assert((op - m_pos) >= 4);
#else
            if (t >= 2 * 4 - (3 - 1) && (op - m_pos) >= 4)
            {
#endif
                COPY4(op,m_pos);
                op += 4; m_pos += 4; t -= 4 - (3 - 1);
                do {
                    COPY4(op,m_pos);
                    op += 4; m_pos += 4; t -= 4;
                } while (t >= 4);
                if (t > 0) do *op++ = *m_pos++; while (--t > 0);
            }
            else
#endif
            {
copy_match:
                *op++ = *m_pos++; *op++ = *m_pos++;
                do *op++ = *m_pos++; while (--t > 0);
            }

#endif

match_done:
#if defined(LZO1Z)
            t = ip[-1] & 3;
#else
            t = ip[-2] & 3;
#endif
            if (t == 0)
                break;

match_next:
            assert(t > 0); assert(t < 4); NEED_OP(t); NEED_IP(t+1);
#if 0
            do *op++ = *ip++; while (--t > 0);
#else
            *op++ = *ip++;
            if (t > 1) { *op++ = *ip++; if (t > 2) { *op++ = *ip++; } }
#endif
            t = *ip++;
        } while (TEST_IP && TEST_OP);
    }

#if defined(HAVE_TEST_IP) || defined(HAVE_TEST_OP)
    *out_len = pd(op, out);
    return LZO_E_EOF_NOT_FOUND;
#endif

eof_found:
    assert(t == 1);
    *out_len = pd(op, out);
    return (ip == ip_end ? LZO_E_OK :
           (ip < ip_end  ? LZO_E_INPUT_NOT_CONSUMED : LZO_E_INPUT_OVERRUN));

#if defined(HAVE_NEED_IP)
input_overrun:
    *out_len = pd(op, out);
    return LZO_E_INPUT_OVERRUN;
#endif

#if defined(HAVE_NEED_OP)
output_overrun:
    *out_len = pd(op, out);
    return LZO_E_OUTPUT_OVERRUN;
#endif

#if defined(LZO_TEST_OVERRUN_LOOKBEHIND)
lookbehind_overrun:
    *out_len = pd(op, out);
    return LZO_E_LOOKBEHIND_OVERRUN;
#endif
}

#endif

#define LZO_TEST_OVERRUN
#undef DO_DECOMPRESS
#define DO_DECOMPRESS       lzo1x_decompress_safe

#if !defined(MINILZO_CFG_SKIP_LZO1X_DECOMPRESS_SAFE)

#if defined(LZO_TEST_OVERRUN)
#  if !defined(LZO_TEST_OVERRUN_INPUT)
#    define LZO_TEST_OVERRUN_INPUT       2
#  endif
#  if !defined(LZO_TEST_OVERRUN_OUTPUT)
#    define LZO_TEST_OVERRUN_OUTPUT      2
#  endif
#  if !defined(LZO_TEST_OVERRUN_LOOKBEHIND)
#    define LZO_TEST_OVERRUN_LOOKBEHIND
#  endif
#endif

#undef TEST_IP
#undef TEST_OP
#undef TEST_LB
#undef TEST_LBO
#undef NEED_IP
#undef NEED_OP
#undef HAVE_TEST_IP
#undef HAVE_TEST_OP
#undef HAVE_NEED_IP
#undef HAVE_NEED_OP
#undef HAVE_ANY_IP
#undef HAVE_ANY_OP

#if defined(LZO_TEST_OVERRUN_INPUT)
#  if (LZO_TEST_OVERRUN_INPUT >= 1)
#    define TEST_IP             (ip < ip_end)
#  endif
#  if (LZO_TEST_OVERRUN_INPUT >= 2)
#    define NEED_IP(x) \
            if ((lzo_uint)(ip_end - ip) < (lzo_uint)(x))  goto input_overrun
#  endif
#endif

#if defined(LZO_TEST_OVERRUN_OUTPUT)
#  if (LZO_TEST_OVERRUN_OUTPUT >= 1)
#    define TEST_OP             (op <= op_end)
#  endif
#  if (LZO_TEST_OVERRUN_OUTPUT >= 2)
#    undef TEST_OP
#    define NEED_OP(x) \
            if ((lzo_uint)(op_end - op) < (lzo_uint)(x))  goto output_overrun
#  endif
#endif

#if defined(LZO_TEST_OVERRUN_LOOKBEHIND)
#  define TEST_LB(m_pos)        if (m_pos < out || m_pos >= op) goto lookbehind_overrun
#  define TEST_LBO(m_pos,o)     if (m_pos < out || m_pos >= op - (o)) goto lookbehind_overrun
#else
#  define TEST_LB(m_pos)        ((void) 0)
#  define TEST_LBO(m_pos,o)     ((void) 0)
#endif

#if !defined(LZO_EOF_CODE) && !defined(TEST_IP)
#  define TEST_IP               (ip < ip_end)
#endif

#if defined(TEST_IP)
#  define HAVE_TEST_IP
#else
#  define TEST_IP               1
#endif
#if defined(TEST_OP)
#  define HAVE_TEST_OP
#else
#  define TEST_OP               1
#endif

#if defined(NEED_IP)
#  define HAVE_NEED_IP
#else
#  define NEED_IP(x)            ((void) 0)
#endif
#if defined(NEED_OP)
#  define HAVE_NEED_OP
#else
#  define NEED_OP(x)            ((void) 0)
#endif

#if defined(HAVE_TEST_IP) || defined(HAVE_NEED_IP)
#  define HAVE_ANY_IP
#endif
#if defined(HAVE_TEST_OP) || defined(HAVE_NEED_OP)
#  define HAVE_ANY_OP
#endif

#undef __COPY4
#define __COPY4(dst,src)    * (lzo_uint32p)(dst) = * (const lzo_uint32p)(src)

#undef COPY4
#if defined(LZO_UNALIGNED_OK_4)
#  define COPY4(dst,src)    __COPY4(dst,src)
#elif defined(LZO_ALIGNED_OK_4)
#  define COPY4(dst,src)    __COPY4((lzo_uintptr_t)(dst),(lzo_uintptr_t)(src))
#endif

#if defined(DO_DECOMPRESS)
LZO_PUBLIC(int)
DO_DECOMPRESS  ( const lzo_bytep in , lzo_uint  in_len,
                       lzo_bytep out, lzo_uintp out_len,
                       lzo_voidp wrkmem )
#endif
{
    register lzo_bytep op;
    register const lzo_bytep ip;
    register lzo_uint t;
#if defined(COPY_DICT)
    lzo_uint m_off;
    const lzo_bytep dict_end;
#else
    register const lzo_bytep m_pos;
#endif

    const lzo_bytep const ip_end = in + in_len;
#if defined(HAVE_ANY_OP)
    lzo_bytep const op_end = out + *out_len;
#endif
#if defined(LZO1Z)
    lzo_uint last_m_off = 0;
#endif

    LZO_UNUSED(wrkmem);

#if defined(COPY_DICT)
    if (dict)
    {
        if (dict_len > M4_MAX_OFFSET)
        {
            dict += dict_len - M4_MAX_OFFSET;
            dict_len = M4_MAX_OFFSET;
        }
        dict_end = dict + dict_len;
    }
    else
    {
        dict_len = 0;
        dict_end = NULL;
    }
#endif

    *out_len = 0;

    op = out;
    ip = in;

    if (*ip > 17)
    {
        t = *ip++ - 17;
        if (t < 4)
            goto match_next;
        assert(t > 0); NEED_OP(t); NEED_IP(t+1);
        do *op++ = *ip++; while (--t > 0);
        goto first_literal_run;
    }

    while (TEST_IP && TEST_OP)
    {
        t = *ip++;
        if (t >= 16)
            goto match;
        if (t == 0)
        {
            NEED_IP(1);
            while (*ip == 0)
            {
                t += 255;
                ip++;
                NEED_IP(1);
            }
            t += 15 + *ip++;
        }
        assert(t > 0); NEED_OP(t+3); NEED_IP(t+4);
#if defined(LZO_UNALIGNED_OK_4) || defined(LZO_ALIGNED_OK_4)
#if !defined(LZO_UNALIGNED_OK_4)
        if (PTR_ALIGNED2_4(op,ip))
        {
#endif
        COPY4(op,ip);
        op += 4; ip += 4;
        if (--t > 0)
        {
            if (t >= 4)
            {
                do {
                    COPY4(op,ip);
                    op += 4; ip += 4; t -= 4;
                } while (t >= 4);
                if (t > 0) do *op++ = *ip++; while (--t > 0);
            }
            else
                do *op++ = *ip++; while (--t > 0);
        }
#if !defined(LZO_UNALIGNED_OK_4)
        }
        else
#endif
#endif
#if !defined(LZO_UNALIGNED_OK_4)
        {
            *op++ = *ip++; *op++ = *ip++; *op++ = *ip++;
            do *op++ = *ip++; while (--t > 0);
        }
#endif

first_literal_run:

        t = *ip++;
        if (t >= 16)
            goto match;
#if defined(COPY_DICT)
#if defined(LZO1Z)
        m_off = (1 + M2_MAX_OFFSET) + (t << 6) + (*ip++ >> 2);
        last_m_off = m_off;
#else
        m_off = (1 + M2_MAX_OFFSET) + (t >> 2) + (*ip++ << 2);
#endif
        NEED_OP(3);
        t = 3; COPY_DICT(t,m_off)
#else
#if defined(LZO1Z)
        t = (1 + M2_MAX_OFFSET) + (t << 6) + (*ip++ >> 2);
        m_pos = op - t;
        last_m_off = t;
#else
        m_pos = op - (1 + M2_MAX_OFFSET);
        m_pos -= t >> 2;
        m_pos -= *ip++ << 2;
#endif
        TEST_LB(m_pos); NEED_OP(3);
        *op++ = *m_pos++; *op++ = *m_pos++; *op++ = *m_pos;
#endif
        goto match_done;

        do {
match:
            if (t >= 64)
            {
#if defined(COPY_DICT)
#if defined(LZO1X)
                m_off = 1 + ((t >> 2) & 7) + (*ip++ << 3);
                t = (t >> 5) - 1;
#elif defined(LZO1Y)
                m_off = 1 + ((t >> 2) & 3) + (*ip++ << 2);
                t = (t >> 4) - 3;
#elif defined(LZO1Z)
                m_off = t & 0x1f;
                if (m_off >= 0x1c)
                    m_off = last_m_off;
                else
                {
                    m_off = 1 + (m_off << 6) + (*ip++ >> 2);
                    last_m_off = m_off;
                }
                t = (t >> 5) - 1;
#endif
#else
#if defined(LZO1X)
                m_pos = op - 1;
                m_pos -= (t >> 2) & 7;
                m_pos -= *ip++ << 3;
                t = (t >> 5) - 1;
#elif defined(LZO1Y)
                m_pos = op - 1;
                m_pos -= (t >> 2) & 3;
                m_pos -= *ip++ << 2;
                t = (t >> 4) - 3;
#elif defined(LZO1Z)
                {
                    lzo_uint off = t & 0x1f;
                    m_pos = op;
                    if (off >= 0x1c)
                    {
                        assert(last_m_off > 0);
                        m_pos -= last_m_off;
                    }
                    else
                    {
                        off = 1 + (off << 6) + (*ip++ >> 2);
                        m_pos -= off;
                        last_m_off = off;
                    }
                }
                t = (t >> 5) - 1;
#endif
                TEST_LB(m_pos); assert(t > 0); NEED_OP(t+3-1);
                goto copy_match;
#endif
            }
            else if (t >= 32)
            {
                t &= 31;
                if (t == 0)
                {
                    NEED_IP(1);
                    while (*ip == 0)
                    {
                        t += 255;
                        ip++;
                        NEED_IP(1);
                    }
                    t += 31 + *ip++;
                }
#if defined(COPY_DICT)
#if defined(LZO1Z)
                m_off = 1 + (ip[0] << 6) + (ip[1] >> 2);
                last_m_off = m_off;
#else
                m_off = 1 + (ip[0] >> 2) + (ip[1] << 6);
#endif
#else
#if defined(LZO1Z)
                {
                    lzo_uint off = 1 + (ip[0] << 6) + (ip[1] >> 2);
                    m_pos = op - off;
                    last_m_off = off;
                }
#elif defined(LZO_UNALIGNED_OK_2) && defined(LZO_ABI_LITTLE_ENDIAN)
                m_pos = op - 1;
                m_pos -= (* (const lzo_ushortp) ip) >> 2;
#else
                m_pos = op - 1;
                m_pos -= (ip[0] >> 2) + (ip[1] << 6);
#endif
#endif
                ip += 2;
            }
            else if (t >= 16)
            {
#if defined(COPY_DICT)
                m_off = (t & 8) << 11;
#else
                m_pos = op;
                m_pos -= (t & 8) << 11;
#endif
                t &= 7;
                if (t == 0)
                {
                    NEED_IP(1);
                    while (*ip == 0)
                    {
                        t += 255;
                        ip++;
                        NEED_IP(1);
                    }
                    t += 7 + *ip++;
                }
#if defined(COPY_DICT)
#if defined(LZO1Z)
                m_off += (ip[0] << 6) + (ip[1] >> 2);
#else
                m_off += (ip[0] >> 2) + (ip[1] << 6);
#endif
                ip += 2;
                if (m_off == 0)
                    goto eof_found;
                m_off += 0x4000;
#if defined(LZO1Z)
                last_m_off = m_off;
#endif
#else
#if defined(LZO1Z)
                m_pos -= (ip[0] << 6) + (ip[1] >> 2);
#elif defined(LZO_UNALIGNED_OK_2) && defined(LZO_ABI_LITTLE_ENDIAN)
                m_pos -= (* (const lzo_ushortp) ip) >> 2;
#else
                m_pos -= (ip[0] >> 2) + (ip[1] << 6);
#endif
                ip += 2;
                if (m_pos == op)
                    goto eof_found;
                m_pos -= 0x4000;
#if defined(LZO1Z)
                last_m_off = pd((const lzo_bytep)op, m_pos);
#endif
#endif
            }
            else
            {
#if defined(COPY_DICT)
#if defined(LZO1Z)
                m_off = 1 + (t << 6) + (*ip++ >> 2);
                last_m_off = m_off;
#else
                m_off = 1 + (t >> 2) + (*ip++ << 2);
#endif
                NEED_OP(2);
                t = 2; COPY_DICT(t,m_off)
#else
#if defined(LZO1Z)
                t = 1 + (t << 6) + (*ip++ >> 2);
                m_pos = op - t;
                last_m_off = t;
#else
                m_pos = op - 1;
                m_pos -= t >> 2;
                m_pos -= *ip++ << 2;
#endif
                TEST_LB(m_pos); NEED_OP(2);
                *op++ = *m_pos++; *op++ = *m_pos;
#endif
                goto match_done;
            }

#if defined(COPY_DICT)

            NEED_OP(t+3-1);
            t += 3-1; COPY_DICT(t,m_off)

#else

            TEST_LB(m_pos); assert(t > 0); NEED_OP(t+3-1);
#if defined(LZO_UNALIGNED_OK_4) || defined(LZO_ALIGNED_OK_4)
#if !defined(LZO_UNALIGNED_OK_4)
            if (t >= 2 * 4 - (3 - 1) && PTR_ALIGNED2_4(op,m_pos))
            {
                assert((op - m_pos) >= 4);
#else
            if (t >= 2 * 4 - (3 - 1) && (op - m_pos) >= 4)
            {
#endif
                COPY4(op,m_pos);
                op += 4; m_pos += 4; t -= 4 - (3 - 1);
                do {
                    COPY4(op,m_pos);
                    op += 4; m_pos += 4; t -= 4;
                } while (t >= 4);
                if (t > 0) do *op++ = *m_pos++; while (--t > 0);
            }
            else
#endif
            {
copy_match:
                *op++ = *m_pos++; *op++ = *m_pos++;
                do *op++ = *m_pos++; while (--t > 0);
            }

#endif

match_done:
#if defined(LZO1Z)
            t = ip[-1] & 3;
#else
            t = ip[-2] & 3;
#endif
            if (t == 0)
                break;

match_next:
            assert(t > 0); assert(t < 4); NEED_OP(t); NEED_IP(t+1);
#if 0
            do *op++ = *ip++; while (--t > 0);
#else
            *op++ = *ip++;
            if (t > 1) { *op++ = *ip++; if (t > 2) { *op++ = *ip++; } }
#endif
            t = *ip++;
        } while (TEST_IP && TEST_OP);
    }

#if defined(HAVE_TEST_IP) || defined(HAVE_TEST_OP)
    *out_len = pd(op, out);
    return LZO_E_EOF_NOT_FOUND;
#endif

eof_found:
    assert(t == 1);
    *out_len = pd(op, out);
    return (ip == ip_end ? LZO_E_OK :
           (ip < ip_end  ? LZO_E_INPUT_NOT_CONSUMED : LZO_E_INPUT_OVERRUN));

#if defined(HAVE_NEED_IP)
input_overrun:
    *out_len = pd(op, out);
    return LZO_E_INPUT_OVERRUN;
#endif

#if defined(HAVE_NEED_OP)
output_overrun:
    *out_len = pd(op, out);
    return LZO_E_OUTPUT_OVERRUN;
#endif

#if defined(LZO_TEST_OVERRUN_LOOKBEHIND)
lookbehind_overrun:
    *out_len = pd(op, out);
    return LZO_E_LOOKBEHIND_OVERRUN;
#endif
}

#endif

/***** End of minilzo.c *****/

