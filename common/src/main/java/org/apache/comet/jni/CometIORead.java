package org.apache.comet.jni;

/**
 * A java wrapper for rust std::io::Read
 */
public interface CometIORead {

    int read(long addr, int size);

    void close();

}
