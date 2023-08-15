package com.xingyuan.sql.gremlin.adapter.converter.ast.nodes.operands;

import com.xingyuan.sql.gremlin.adapter.converter.SqlMetadata;
import com.xingyuan.sql.gremlin.adapter.converter.ast.nodes.GremlinSqlNode;
import org.apache.calcite.sql.SqlIdentifier;

import java.sql.SQLException;

/**
 * This module is a GremlinSql equivalent of Calcite's SqlIdentifier.
 */
public class GremlinSqlIdentifier extends GremlinSqlNode {
    private final SqlIdentifier sqlIdentifier;

    public GremlinSqlIdentifier(final SqlIdentifier sqlIdentifier, final SqlMetadata sqlMetadata) {
        super(sqlIdentifier, sqlMetadata);
        this.sqlIdentifier = sqlIdentifier;
    }


    public String getName(final int idx) throws SQLException {
        if (idx >= sqlIdentifier.names.size()) {
            throw new SQLException("Index of identifier > size of name list for identifier");
        }
        return sqlIdentifier.names.get(idx);
    }


    public String getColumn() throws SQLException {
        if (sqlIdentifier.names.size() < 1) {
            throw new SQLException("Expected at least one name in list for identifier");
        }
        return sqlIdentifier.names.get(sqlIdentifier.names.size() - 1);
    }
}
