For the latest information about Flink, please visit our website at:

   https://flink.apache.org

and our GitHub Account 

   https://github.com/apache/flink

If you have any questions, ask on our Mailing lists:

   user@flink.apache.org
   dev@flink.apache.org

This distribution includes cryptographic software.  The country in
which you currently reside may have restrictions on the import,
possession, use, and/or re-export to another country, of
encryption software. BEFORE using any encryption software, please
check your country's laws, regulations and policies concerning the
import, possession, or use, and re-export of encryption software, to
see if this is permitted. See http://www.wassenaar.org for
more information.

The Apache Software Foundation has classified this software as Export
Commodity Control Number (ECCN) 5D002, which includes information
security software using or performing cryptographic functions with
asymmetric algorithms. The form and manner of this Apache Software
Foundation distribution makes it eligible for export under the
"publicly available" Section 742.15(b) exemption (see the BIS Export
Administration Regulations, Section 742.15(b)) for both object code
and source code.

The following provides more details on the included cryptographic
software:

  * Apache Flink uses the built-in Java TLS/SSL (JSSE) libraries to
    secure network communication (RPC, REST, blob transfer, and
    connector traffic). Alternatively, the OpenSSL-based SSL engine
    can be enabled (security.ssl.provider: OPENSSL), using the
    flink-shaded-netty-tcnative packaging of OpenSSL/BoringSSL.
