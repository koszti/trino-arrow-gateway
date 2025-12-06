package com.wise.streamprocessing.trinoarrowgateway.trino;

public interface QueryRegistry {

    void register(TrinoQueryHandle handle);

    TrinoQueryHandle get(String queryId);
}
