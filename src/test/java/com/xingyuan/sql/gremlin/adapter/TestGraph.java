package com.xingyuan.sql.gremlin.adapter;

import org.apache.tinkerpop.gremlin.structure.Graph;

public interface TestGraph {
    void populate(Graph graph);
}
