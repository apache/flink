/* XzEnc.c -- Xz Encode
2009-06-04 : Igor Pavlov : Public domain */

#include <stdlib.h>
#include <string.h>

#include "7zCrc.h"
#include "Alloc.h"
#include "Bra.h"
#include "CpuArch.h"
#ifdef USE_SUBBLOCK
#include "SbEnc.h"
#endif

#include "XzEnc.h"

static void *SzBigAlloc(void *p, size_t size) { p = p; return BigAlloc(size); }
static void SzBigFree(void *p, void *address) { p = p; BigFree(address); }
static ISzAlloc g_BigAlloc = { SzBigAlloc, SzBigFree };

static void *SzAlloc(void *p, size_t size) { p = p; return MyAlloc(size); }
static void SzFree(void *p, void *address) { p = p; MyFree(address); }
static ISzAlloc g_Alloc = { SzAlloc, SzFree };

#define XzBlock_ClearFlags(p)       (p)->flags = 0;
#define XzBlock_SetNumFilters(p, n) (p)->flags |= ((n) - 1);
#define XzBlock_SetHasPackSize(p)   (p)->flags |= XZ_BF_PACK_SIZE;
#define XzBlock_SetHasUnpackSize(p) (p)->flags |= XZ_BF_UNPACK_SIZE;

static SRes WriteBytes(ISeqOutStream *s, const void *buf, UInt32 size)
{
  return (s->Write(s, buf, size) == size) ? SZ_OK : SZ_ERROR_WRITE;
}

static SRes WriteBytesAndCrc(ISeqOutStream *s, const void *buf, UInt32 size, UInt32 *crc)
{
  *crc = CrcUpdate(*crc, buf, size);
  return WriteBytes(s, buf, size);
}

SRes Xz_WriteHeader(CXzStreamFlags f, ISeqOutStream *s)
{
  UInt32 crc;
  Byte header[XZ_STREAM_HEADER_SIZE];
  memcpy(header, XZ_SIG, XZ_SIG_SIZE);
  header[XZ_SIG_SIZE] = (Byte)(f >> 8);
  header[XZ_SIG_SIZE + 1] = (Byte)(f & 0xFF);
  crc = CrcCalc(header + XZ_SIG_SIZE, XZ_STREAM_FLAGS_SIZE);
  SetUi32(header + XZ_SIG_SIZE + XZ_STREAM_FLAGS_SIZE, crc);
  return WriteBytes(s, header, XZ_STREAM_HEADER_SIZE);
}

SRes XzBlock_WriteHeader(const CXzBlock *p, ISeqOutStream *s)
{
  Byte header[XZ_BLOCK_HEADER_SIZE_MAX];

  unsigned pos = 1;
  int numFilters, i;
  header[pos++] = p->flags;

  if (XzBlock_HasPackSize(p)) pos += Xz_WriteVarInt(header + pos, p->packSize);
  if (XzBlock_HasUnpackSize(p)) pos += Xz_WriteVarInt(header + pos, p->unpackSize);
  numFilters = XzBlock_GetNumFilters(p);
  for (i = 0; i < numFilters; i++)
  {
    const CXzFilter *f = &p->filters[i];
    pos += Xz_WriteVarInt(header + pos, f->id);
    pos += Xz_WriteVarInt(header + pos, f->propsSize);
    memcpy(header + pos, f->props, f->propsSize);
    pos += f->propsSize;
  }
  while((pos & 3) != 0)
    header[pos++] = 0;
  header[0] = (Byte)(pos >> 2);
  SetUi32(header + pos, CrcCalc(header, pos));
  return WriteBytes(s, header, pos + 4);
}

SRes Xz_WriteFooter(CXzStream *p, ISeqOutStream *s)
{
  Byte buf[32];
  UInt64 globalPos;
  {
    UInt32 crc = CRC_INIT_VAL;
    unsigned pos = 1 + Xz_WriteVarInt(buf + 1, p->numBlocks);
    size_t i;

    globalPos = pos;
    buf[0] = 0;
    RINOK(WriteBytesAndCrc(s, buf, pos, &crc));
    for (i = 0; i < p->numBlocks; i++)
    {
      const CXzBlockSizes *block = &p->blocks[i];
      pos = Xz_WriteVarInt(buf, block->totalSize);
      pos += Xz_WriteVarInt(buf + pos, block->unpackSize);
      globalPos += pos;
      RINOK(WriteBytesAndCrc(s, buf, pos, &crc));
    }
    pos = ((unsigned)globalPos & 3);
    if (pos != 0)
    {
      buf[0] = buf[1] = buf[2] = 0;
      RINOK(WriteBytesAndCrc(s, buf, 4 - pos, &crc));
      globalPos += 4 - pos;
    }
    {
      SetUi32(buf, CRC_GET_DIGEST(crc));
      RINOK(WriteBytes(s, buf, 4));
      globalPos += 4;
    }
  }

  {
    UInt32 indexSize = (UInt32)((globalPos >> 2) - 1);
    SetUi32(buf + 4, indexSize);
    buf[8] = (Byte)(p->flags >> 8);
    buf[9] = (Byte)(p->flags & 0xFF);
    SetUi32(buf, CrcCalc(buf + 4, 6));
    memcpy(buf + 10, XZ_FOOTER_SIG, XZ_FOOTER_SIG_SIZE);
    return WriteBytes(s, buf, 12);
  }
}

SRes Xz_AddIndexRecord(CXzStream *p, UInt64 unpackSize, UInt64 totalSize, ISzAlloc *alloc)
{
  if (p->blocks == 0 || p->numBlocksAllocated == p->numBlocks)
  {
    size_t num = (p->numBlocks + 1) * 2;
    size_t newSize = sizeof(CXzBlockSizes) * num;
    CXzBlockSizes *blocks;
    if (newSize / sizeof(CXzBlockSizes) != num)
      return SZ_ERROR_MEM;
    blocks = alloc->Alloc(alloc, newSize);
    if (blocks == 0)
      return SZ_ERROR_MEM;
    if (p->numBlocks != 0)
    {
      memcpy(blocks, p->blocks, p->numBlocks * sizeof(CXzBlockSizes));
      Xz_Free(p, alloc);
    }
    p->blocks = blocks;
    p->numBlocksAllocated = num;
  }
  {
    CXzBlockSizes *block = &p->blocks[p->numBlocks++];
    block->totalSize = totalSize;
    block->unpackSize = unpackSize;
  }
  return SZ_OK;
}

/* ---------- CSeqCheckInStream ---------- */

typedef struct
{
  ISeqInStream p;
  ISeqInStream *realStream;
  UInt64 processed;
  CXzCheck check;
} CSeqCheckInStream;

void SeqCheckInStream_Init(CSeqCheckInStream *p, int mode)
{
  p->processed = 0;
  XzCheck_Init(&p->check, mode);
}

void SeqCheckInStream_GetDigest(CSeqCheckInStream *p, Byte *digest)
{
  XzCheck_Final(&p->check, digest);
}

static SRes SeqCheckInStream_Read(void *pp, void *data, size_t *size)
{
  CSeqCheckInStream *p = (CSeqCheckInStream *)pp;
  SRes res = p->realStream->Read(p->realStream, data, size);
  XzCheck_Update(&p->check, data, *size);
  p->processed += *size;
  return res;
}

/* ---------- CSeqSizeOutStream ---------- */

typedef struct
{
  ISeqOutStream p;
  ISeqOutStream *realStream;
  UInt64 processed;
} CSeqSizeOutStream;

static size_t MyWrite(void *pp, const void *data, size_t size)
{
  CSeqSizeOutStream *p = (CSeqSizeOutStream *)pp;
  size = p->realStream->Write(p->realStream, data, size);
  p->processed += size;
  return size;
}

/* ---------- CSeqInFilter ---------- */

/*
typedef struct _IFilter
{
  void *p;
  void (*Free)(void *p, ISzAlloc *alloc);
  SRes (*SetProps)(void *p, const Byte *props, size_t propSize, ISzAlloc *alloc);
  void (*Init)(void *p);
  size_t (*Filter)(void *p, Byte *data, SizeT destLen);
} IFilter;

#define FILT_BUF_SIZE (1 << 19)

typedef struct
{
  ISeqInStream p;
  ISeqInStream *realStream;
  UInt32 x86State;
  UInt32 ip;
  UInt64 processed;
  CXzCheck check;
  Byte buf[FILT_BUF_SIZE];
  UInt32 bufferPos;
  UInt32 convertedPosBegin;
  UInt32 convertedPosEnd;
  IFilter *filter;
} CSeqInFilter;

static SRes SeqInFilter_Read(void *pp, void *data, size_t *size)
{
  CSeqInFilter *p = (CSeqInFilter *)pp;
  size_t remSize = *size;
  *size = 0;

  while (remSize > 0)
  {
    int i;
    if (p->convertedPosBegin != p->convertedPosEnd)
    {
      UInt32 sizeTemp = p->convertedPosEnd - p->convertedPosBegin;
      if (remSize < sizeTemp)
        sizeTemp = (UInt32)remSize;
      memmove(data, p->buf + p->convertedPosBegin, sizeTemp);
      p->convertedPosBegin += sizeTemp;
      data = (void *)((Byte *)data + sizeTemp);
      remSize -= sizeTemp;
      *size += sizeTemp;
      break;
    }
    for (i = 0; p->convertedPosEnd + i < p->bufferPos; i++)
      p->buf[i] = p->buf[i + p->convertedPosEnd];
    p->bufferPos = i;
    p->convertedPosBegin = p->convertedPosEnd = 0;
    {
      size_t processedSizeTemp = FILT_BUF_SIZE - p->bufferPos;
      RINOK(p->realStream->Read(p->realStream, p->buf + p->bufferPos, &processedSizeTemp));
      p->bufferPos = p->bufferPos + (UInt32)processedSizeTemp;
    }
    p->convertedPosEnd = (UInt32)p->filter->Filter(p->filter->p, p->buf, p->bufferPos);
    if (p->convertedPosEnd == 0)
    {
      if (p->bufferPos == 0)
        break;
      else
      {
        p->convertedPosEnd = p->bufferPos;
        continue;
      }
    }
    if (p->convertedPosEnd > p->bufferPos)
    {
      for (; p->bufferPos < p->convertedPosEnd; p->bufferPos++)
        p->buf[p->bufferPos] = 0;
      p->convertedPosEnd = (UInt32)p->filter->Filter(p->filter->p, p->buf, p->bufferPos);
    }
  }
  return SZ_OK;
}
*/

/*
typedef struct
{
  ISeqInStream p;
  ISeqInStream *realStream;
  CMixCoder mixCoder;
  Byte buf[FILT_BUF_SIZE];
  UInt32 bufPos;
  UInt32 bufSize;
} CMixCoderSeqInStream;

static SRes CMixCoderSeqInStream_Read(void *pp, void *data, size_t *size)
{
  CMixCoderSeqInStream *p = (CMixCoderSeqInStream *)pp;
  SRes res = SZ_OK;
  size_t remSize = *size;
  *size = 0;
  while (remSize > 0)
  {
    if (p->bufPos == p->bufSize)
    {
      size_t curSize;
      p->bufPos = p->bufSize = 0;
      if (*size != 0)
        break;
      curSize = FILT_BUF_SIZE;
      RINOK(p->realStream->Read(p->realStream, p->buf, &curSize));
      p->bufSize = (UInt32)curSize;
    }
    {
      SizeT destLen = remSize;
      SizeT srcLen = p->bufSize - p->bufPos;
      res = MixCoder_Code(&p->mixCoder, data, &destLen, p->buf + p->bufPos, &srcLen, 0);
      data = (void *)((Byte *)data + destLen);
      remSize -= destLen;
      *size += destLen;
      p->bufPos += srcLen;
    }
  }
  return res;
}
*/

#ifdef USE_SUBBLOCK
typedef struct
{
  ISeqInStream p;
  CSubblockEnc sb;
  UInt64 processed;
} CSbEncInStream;

void SbEncInStream_Init(CSbEncInStream *p)
{
  p->processed = 0;
  SubblockEnc_Init(&p->sb);
}

static SRes SbEncInStream_Read(void *pp, void *data, size_t *size)
{
  CSbEncInStream *p = (CSbEncInStream *)pp;
  SRes res = SubblockEnc_Read(&p->sb, data, size);
  p->processed += *size;
  return res;
}
#endif

typedef struct
{
  /* CMixCoderSeqInStream inStream; */
  CLzma2EncHandle lzma2;
  #ifdef USE_SUBBLOCK
  CSbEncInStream sb;
  #endif
  ISzAlloc *alloc;
  ISzAlloc *bigAlloc;
} CLzma2WithFilters;


static void Lzma2WithFilters_Construct(CLzma2WithFilters *p, ISzAlloc *alloc, ISzAlloc *bigAlloc)
{
  p->alloc = alloc;
  p->bigAlloc = bigAlloc;
  p->lzma2 = NULL;
  #ifdef USE_SUBBLOCK
  p->sb.p.Read = SbEncInStream_Read;
  SubblockEnc_Construct(&p->sb.sb, p->alloc);
  #endif
}

static SRes Lzma2WithFilters_Create(CLzma2WithFilters *p)
{
  p->lzma2 = Lzma2Enc_Create(p->alloc, p->bigAlloc);
  if (p->lzma2 == 0)
    return SZ_ERROR_MEM;
  return SZ_OK;
}

static void Lzma2WithFilters_Free(CLzma2WithFilters *p)
{
  #ifdef USE_SUBBLOCK
  SubblockEnc_Free(&p->sb.sb);
  #endif
  if (p->lzma2)
  {
    Lzma2Enc_Destroy(p->lzma2);
    p->lzma2 = NULL;
  }
}

static SRes Xz_Compress(CXzStream *xz,
    CLzma2WithFilters *lzmaf,
    ISeqOutStream *outStream,
    ISeqInStream *inStream,
    const CLzma2EncProps *lzma2Props,
    Bool useSubblock,
    ICompressProgress *progress)
{
  xz->flags = XZ_CHECK_CRC32;

  RINOK(Lzma2Enc_SetProps(lzmaf->lzma2, lzma2Props));
  RINOK(Xz_WriteHeader(xz->flags, outStream));

  {
    CSeqCheckInStream checkInStream;
    CSeqSizeOutStream seqSizeOutStream;
    CXzBlock block;
    int filterIndex = 0;
    
    XzBlock_ClearFlags(&block);
    XzBlock_SetNumFilters(&block, 1 + (useSubblock ? 1 : 0));
    
    if (useSubblock)
    {
      CXzFilter *f = &block.filters[filterIndex++];
      f->id = XZ_ID_Subblock;
      f->propsSize = 0;
    }

    {
      CXzFilter *f = &block.filters[filterIndex++];
      f->id = XZ_ID_LZMA2;
      f->propsSize = 1;
      f->props[0] = Lzma2Enc_WriteProperties(lzmaf->lzma2);
    }

    seqSizeOutStream.p.Write = MyWrite;
    seqSizeOutStream.realStream = outStream;
    seqSizeOutStream.processed = 0;
    
    RINOK(XzBlock_WriteHeader(&block, &seqSizeOutStream.p));
    
    checkInStream.p.Read = SeqCheckInStream_Read;
    checkInStream.realStream = inStream;
    SeqCheckInStream_Init(&checkInStream, XzFlags_GetCheckType(xz->flags));
    
    #ifdef USE_SUBBLOCK
    if (useSubblock)
    {
      lzmaf->sb.sb.inStream = &checkInStream.p;
      SubblockEnc_Init(&lzmaf->sb.sb);
    }
    #endif
    
    {
      UInt64 packPos = seqSizeOutStream.processed;
      SRes res = Lzma2Enc_Encode(lzmaf->lzma2, &seqSizeOutStream.p,
        #ifdef USE_SUBBLOCK
        useSubblock ? &lzmaf->sb.p:
        #endif
        &checkInStream.p,
        progress);
      RINOK(res);
      block.unpackSize = checkInStream.processed;
      block.packSize = seqSizeOutStream.processed - packPos;
    }

    {
      unsigned padSize = 0;
      Byte buf[128];
      while((((unsigned)block.packSize + padSize) & 3) != 0)
        buf[padSize++] = 0;
      SeqCheckInStream_GetDigest(&checkInStream, buf + padSize);
      RINOK(WriteBytes(&seqSizeOutStream.p, buf, padSize + XzFlags_GetCheckSize(xz->flags)));
      RINOK(Xz_AddIndexRecord(xz, block.unpackSize, seqSizeOutStream.processed - padSize, &g_Alloc));
    }
  }
  return Xz_WriteFooter(xz, outStream);
}

SRes Xz_Encode(ISeqOutStream *outStream, ISeqInStream *inStream,
    const CLzma2EncProps *lzma2Props, Bool useSubblock,
    ICompressProgress *progress)
{
  SRes res;
  CXzStream xz;
  CLzma2WithFilters lzmaf;
  Xz_Construct(&xz);
  Lzma2WithFilters_Construct(&lzmaf, &g_Alloc, &g_BigAlloc);
  res = Lzma2WithFilters_Create(&lzmaf);
  if (res == SZ_OK)
    res = Xz_Compress(&xz, &lzmaf, outStream, inStream,
        lzma2Props, useSubblock, progress);
  Lzma2WithFilters_Free(&lzmaf);
  Xz_Free(&xz, &g_Alloc);
  return res;
}

SRes Xz_EncodeEmpty(ISeqOutStream *outStream)
{
  SRes res;
  CXzStream xz;
  Xz_Construct(&xz);
  res = Xz_WriteHeader(xz.flags, outStream);
  if (res == SZ_OK)
    res = Xz_WriteFooter(&xz, outStream);
  Xz_Free(&xz, &g_Alloc);
  return res;
}
