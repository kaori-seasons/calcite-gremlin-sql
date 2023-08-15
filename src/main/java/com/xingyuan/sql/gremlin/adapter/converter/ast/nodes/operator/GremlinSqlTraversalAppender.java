package com.xingyuan.sql.gremlin.adapter.converter.ast.nodes.operator;

import com.xingyuan.sql.gremlin.adapter.converter.ast.nodes.GremlinSqlNode;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import java.sql.SQLException;
import java.util.List;

/**
 * Interface for traversal appending function.
 */
public interface GremlinSqlTraversalAppender {
    void appendTraversal(GraphTraversal<?, ?> graphTraversal, List<GremlinSqlNode> operands) throws SQLException;
}
