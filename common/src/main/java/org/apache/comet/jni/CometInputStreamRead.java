package org.apache.comet.jni;

import org.apache.comet.CometRuntimeException;
import org.apache.spark.unsafe.Platform;

import java.io.IOException;
import java.io.InputStream;

/**
 * JNI interface for reading from an JVM InputStream in native code.
 */
public class CometInputStreamRead implements CometIORead {


    private InputStream in;

    public CometInputStreamRead(InputStream in) {
        this.in = in;
    }

    @Override
    public int read(long addr, int size) {
        byte[] buffer = new byte[size];
        int bytesRead;
        try {
            bytesRead = in.read(buffer);
        } catch (IOException e) {
            throw new CometRuntimeException("Error on read input stream", e);
        }
        // copy buffer bytes to addr
        if (bytesRead > 0) {
            Platform.copyMemory(buffer, Platform.BYTE_ARRAY_OFFSET, null, addr, bytesRead);
        }
        return bytesRead;
    }

    @Override
    public void close() {
        try {
            in.close();
        } catch (IOException e) {
            throw new CometRuntimeException("Error on close input stream", e);
        }
    }
}
