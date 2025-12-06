package com.wise.streamprocessing.trinoarrowgateway.trino;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class InMemoryQueryRegistry implements QueryRegistry {

    private final Map<String, TrinoQueryHandle> byId = new ConcurrentHashMap<>();

    @Override
    public void register(TrinoQueryHandle handle) {
        byId.put(handle.getQueryId(), handle);
    }

    @Override
    public TrinoQueryHandle get(String queryId) {
        return byId.get(queryId);
    }
}
