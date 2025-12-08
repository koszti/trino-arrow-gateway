package io.github.koszti.trinoarrowgateway.trino;

public interface QueryRegistry {

    void register(TrinoQueryHandle handle);

    TrinoQueryHandle get(String queryId);
}
