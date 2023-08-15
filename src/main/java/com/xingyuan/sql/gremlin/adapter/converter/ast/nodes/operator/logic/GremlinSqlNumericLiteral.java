package com.xingyuan.sql.gremlin.adapter.converter.ast.nodes.operator.logic;

import com.xingyuan.sql.gremlin.adapter.converter.SqlMetadata;
import com.xingyuan.sql.gremlin.adapter.converter.ast.nodes.GremlinSqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import java.sql.SQLException;

/**
 * This module is a GremlinSql equivalent of Calcite's GremlinSqlNumericLiteral.
 */
public class GremlinSqlNumericLiteral extends GremlinSqlNode {
    private final SqlNumericLiteral sqlNumericLiteral;

    public GremlinSqlNumericLiteral(final SqlNumericLiteral sqlNumericLiteral,
                                    final SqlMetadata sqlMetadata) {
        super(sqlNumericLiteral, sqlMetadata);
        this.sqlNumericLiteral = sqlNumericLiteral;
    }

    public void appendTraversal(final GraphTraversal graphTraversal) throws SQLException {
        graphTraversal.constant(getValue());
    }

    public Object getValue() throws SQLException {
        return sqlNumericLiteral.getValue();
    }
}
