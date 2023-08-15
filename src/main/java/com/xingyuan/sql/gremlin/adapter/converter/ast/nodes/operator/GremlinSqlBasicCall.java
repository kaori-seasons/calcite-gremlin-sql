package com.xingyuan.sql.gremlin.adapter.converter.ast.nodes.operator;

import com.xingyuan.sql.gremlin.adapter.converter.SqlMetadata;
import com.xingyuan.sql.gremlin.adapter.converter.ast.nodes.GremlinSqlFactory;
import com.xingyuan.sql.gremlin.adapter.converter.ast.nodes.GremlinSqlNode;
import com.xingyuan.sql.gremlin.adapter.converter.ast.nodes.operands.GremlinSqlIdentifier;
import com.xingyuan.sql.gremlin.adapter.converter.ast.nodes.operator.aggregate.GremlinSqlAggFunction;
import com.xingyuan.sql.gremlin.adapter.converter.ast.nodes.operator.logic.GremlinSqlNumericLiteral;
import lombok.Getter;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

/**
 * This module is a GremlinSql equivalent of Calcite's SqlBasicCall.
 */
@Getter
public class GremlinSqlBasicCall extends GremlinSqlNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlBasicCall.class);
    private final SqlBasicCall sqlBasicCall;
    private final GremlinSqlOperator gremlinSqlOperator;
    private final List<GremlinSqlNode> gremlinSqlNodes;

    public GremlinSqlBasicCall(final SqlBasicCall sqlBasicCall, final SqlMetadata sqlMetadata)
            throws SQLException {
        super(sqlBasicCall, sqlMetadata);
        this.sqlBasicCall = sqlBasicCall;
        gremlinSqlOperator =
                GremlinSqlFactory.createOperator(sqlBasicCall.getOperator(), sqlBasicCall.getOperandList());
        gremlinSqlNodes = GremlinSqlFactory.createNodeList(sqlBasicCall.getOperandList());
    }

    void validate() throws SQLException {
        if (gremlinSqlOperator instanceof GremlinSqlAsOperator) {
            if (gremlinSqlNodes.size() != 2) {
                throw new SQLException("Error, expected only two sub nodes for GremlinSqlBasicCall.");
            }
        } else if (gremlinSqlOperator instanceof GremlinSqlAggFunction) {
            if (gremlinSqlNodes.size() != 1) {
                throw new SQLException("Error, expected only one sub node for GremlinSqlAggFunction.");
            }
        }
    }

    public void generateTraversal(final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        validate();
        gremlinSqlOperator.appendOperatorTraversal(graphTraversal);
    }

    public String getRename() throws SQLException {
        if (gremlinSqlOperator instanceof GremlinSqlAsOperator) {
            return ((GremlinSqlAsOperator) gremlinSqlOperator).getRename();
        } else if (gremlinSqlOperator instanceof GremlinSqlAggFunction) {
            if (gremlinSqlNodes.size() == 1 && gremlinSqlNodes.get(0) instanceof GremlinSqlIdentifier) {
                // returns the formatted column name for aggregations
                return ((GremlinSqlAggFunction) gremlinSqlOperator).getNewName();
            }
        }
        throw new SQLException("Unable to determine column rename.");
    }

    public String getActual() throws SQLException {
        if (gremlinSqlOperator instanceof GremlinSqlAsOperator) {
            return ((GremlinSqlAsOperator) gremlinSqlOperator).getActual();
        } else if (gremlinSqlOperator instanceof GremlinSqlAggFunction) {
            if (gremlinSqlNodes.size() == 1 && gremlinSqlNodes.get(0) instanceof GremlinSqlIdentifier) {
                return ((GremlinSqlIdentifier) gremlinSqlNodes.get(0)).getColumn();
            } else if (gremlinSqlNodes.size() == 2 && gremlinSqlNodes.get(1) instanceof GremlinSqlIdentifier) {
                return ((GremlinSqlIdentifier) gremlinSqlNodes.get(1)).getColumn();
            } else if (gremlinSqlNodes.size() == 1 && gremlinSqlNodes.get(0) instanceof GremlinSqlNumericLiteral) {
                return ((GremlinSqlAggFunction) gremlinSqlOperator).getNewName();
            }
        }
        throw new SQLException("Unable to determine actual column name.");
    }
}
