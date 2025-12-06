package com.wise.streamprocessing.trinoarrowgateway.spool;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Abstraction over where result spool files live (S3, local FS, etc).
 */
public interface SpoolSource
{
    /**
     * List all spool objects under a given prefix (relative to the underlying store).
     * For S3, this is bucket + prefix + given argument.
     */
    List<SpoolObject> listObjects(String relativePrefix) throws IOException;

    /**
     * Open an InputStream for the given spool object.
     * Caller is responsible for closing the stream.
     */
    InputStream openObject(SpoolObject object) throws IOException;
}
