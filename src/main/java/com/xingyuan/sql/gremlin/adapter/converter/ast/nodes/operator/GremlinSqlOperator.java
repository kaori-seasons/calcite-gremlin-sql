package com.xingyuan.sql.gremlin.adapter.converter.ast.nodes.operator;

import com.xingyuan.sql.gremlin.adapter.converter.SqlMetadata;
import com.xingyuan.sql.gremlin.adapter.converter.ast.nodes.GremlinSqlNode;
import lombok.AllArgsConstructor;
import org.apache.calcite.sql.SqlOperator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

/**
 * This abstract class is a GremlinSql equivalent of Calcite's SqlOperator.
 */
@AllArgsConstructor
public abstract class GremlinSqlOperator {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlOperator.class);
    private final SqlOperator sqlOperator;
    private final List<GremlinSqlNode> sqlOperands;
    private final SqlMetadata sqlMetadata;

    protected abstract void appendTraversal(GraphTraversal<?, ?> graphTraversal) throws SQLException;

    public void appendOperatorTraversal(final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        if (sqlOperands.size() > 2) {
            throw new SQLException("Error: Expected 2 or less operands in operations.");
        } else if (sqlOperands.isEmpty()) {
            throw new SQLException("Error: Expected at least 1 operand in operations.");
        }

        appendTraversal(graphTraversal);
    }
}
