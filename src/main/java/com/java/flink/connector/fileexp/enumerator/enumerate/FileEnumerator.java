package com.java.flink.connector.fileexp.enumerator.enumerate;

import org.apache.flink.core.fs.Path;

import com.java.flink.connector.fileexp.split.FileSourceSplit;

import java.io.IOException;
import java.util.Collection;

/** */
public interface FileEnumerator {
    /**
     * Generates all file splits for the relevant files under the given paths. The {@code
     * minDesiredSplits} is an optional hint indicating how many splits would be necessary to
     * exploit parallelism properly.
     */
    Collection<FileSourceSplit> enumerateSplits(Path[] paths, int minDesiredSplits)
            throws IOException;
}
