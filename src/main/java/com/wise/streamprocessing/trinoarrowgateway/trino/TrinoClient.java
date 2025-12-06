package com.wise.streamprocessing.trinoarrowgateway.trino;

public interface TrinoClient
{
    /**
     * Submit a SQL query to Trino via /v1/statement.
     * Returns a handle containing the Trino query id and basic column metadata.
     */
    TrinoQueryHandle submitQuery(String sql);
}
