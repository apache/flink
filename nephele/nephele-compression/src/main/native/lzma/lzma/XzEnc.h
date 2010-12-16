/* XzEnc.h -- Xz Encode
2009-04-15 : Igor Pavlov : Public domain */

#ifndef __XZ_ENC_H
#define __XZ_ENC_H

#include "Lzma2Enc.h"

#include "Xz.h"

#ifdef __cplusplus
extern "C" {
#endif

SRes Xz_Encode(ISeqOutStream *outStream, ISeqInStream *inStream,
    const CLzma2EncProps *lzma2Props, Bool useSubblock,
    ICompressProgress *progress);

SRes Xz_EncodeEmpty(ISeqOutStream *outStream);

#ifdef __cplusplus
}
#endif

#endif
