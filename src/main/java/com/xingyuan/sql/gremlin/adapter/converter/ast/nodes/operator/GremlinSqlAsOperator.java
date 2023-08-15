package com.xingyuan.sql.gremlin.adapter.converter.ast.nodes.operator;

import com.xingyuan.sql.gremlin.adapter.converter.SqlMetadata;
import com.xingyuan.sql.gremlin.adapter.converter.SqlTraversalEngine;
import com.xingyuan.sql.gremlin.adapter.converter.ast.nodes.GremlinSqlNode;
import com.xingyuan.sql.gremlin.adapter.converter.ast.nodes.operands.GremlinSqlIdentifier;
import com.xingyuan.sql.gremlin.adapter.converter.ast.nodes.operator.logic.GremlinSqlNumericLiteral;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

/**
 * This module is a GremlinSql equivalent of Calcite's SqlAsOperator.
 */
public class GremlinSqlAsOperator extends GremlinSqlOperator {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlAsOperator.class);
    private final SqlAsOperator sqlAsOperator;
    private final SqlMetadata sqlMetadata;
    private final List<GremlinSqlNode> sqlOperands;

    public GremlinSqlAsOperator(final SqlAsOperator sqlAsOperator, final List<GremlinSqlNode> gremlinSqlNodes,
                                final SqlMetadata sqlMetadata) {
        super(sqlAsOperator, gremlinSqlNodes, sqlMetadata);
        this.sqlAsOperator = sqlAsOperator;
        this.sqlMetadata = sqlMetadata;
        this.sqlOperands = gremlinSqlNodes;
    }

    @Override
    protected void appendTraversal(final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        if (sqlOperands.get(0) instanceof GremlinSqlBasicCall) {
            ((GremlinSqlBasicCall) sqlOperands.get(0)).generateTraversal(graphTraversal);
        } else if (!(sqlOperands.get(0) instanceof GremlinSqlIdentifier) &&
                !(sqlOperands.get(0) instanceof GremlinSqlNumericLiteral)) {
            throw new SQLException(
                    "Error: expected operand to be GremlinSqlBasicCall or GremlinSqlIdentifier in GremlinSqlOperator.");
        }

        if (sqlOperands.size() == 1) {
            if (sqlOperands.get(0) instanceof GremlinSqlIdentifier) {
                SqlTraversalEngine
                        .applySqlIdentifier((GremlinSqlIdentifier) sqlOperands.get(0), sqlMetadata, graphTraversal);
            }
        }
        if (sqlOperands.size() == 2 && sqlOperands.get(0) instanceof GremlinSqlIdentifier) {
            SqlTraversalEngine
                    .applySqlIdentifier((GremlinSqlIdentifier) sqlOperands.get(0), sqlMetadata, graphTraversal);
        }
        sqlMetadata.addRenamedColumn(getActual(), getRename());
    }

    public String getName(final int operandIdx, final int nameIdx) throws SQLException {
        if (operandIdx >= sqlOperands.size() || !(sqlOperands.get(operandIdx) instanceof GremlinSqlIdentifier)) {
            throw new SQLException(
                    "Error: Expected operand idx less than number of operands and GremlinSqlIdentifier for operand");
        }
        return ((GremlinSqlIdentifier) sqlOperands.get(operandIdx)).getName(nameIdx);
    }

    public String getActual() throws SQLException {
        if (sqlOperands.size() != 2) {
            throw new SQLException("Error: Expected two operands for SQL AS statement.");
        }
        if (sqlOperands.get(0) instanceof GremlinSqlIdentifier) {
            return ((GremlinSqlIdentifier) sqlOperands.get(0)).getColumn();
        } else if (sqlOperands.get(0) instanceof GremlinSqlBasicCall) {
            return ((GremlinSqlBasicCall) sqlOperands.get(0)).getActual();
        }
        throw new SQLException("Error, unable to get actual name in GremlinSqlAsOperator.");
    }

    public String getRename() throws SQLException {
        if (sqlOperands.size() != 2) {
            throw new SQLException("Error: Expected two operands for SQL AS statement.");
        }
        if (sqlOperands.get(1) instanceof GremlinSqlIdentifier) {
            return ((GremlinSqlIdentifier) sqlOperands.get(1)).getColumn();
        } else if (sqlOperands.get(1) instanceof GremlinSqlBasicCall) {
            return ((GremlinSqlBasicCall) sqlOperands.get(1)).getRename();
        }
        throw new SQLException("Error, unable to get rename name in GremlinSqlAsOperator.");
    }
}
