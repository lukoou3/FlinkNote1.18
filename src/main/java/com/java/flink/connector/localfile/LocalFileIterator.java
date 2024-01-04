package com.java.flink.connector.localfile;

import java.util.Iterator;

public class LocalFileIterator<OUT> implements Iterator<OUT> {
    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public OUT next() {
        return null;
    }
}
