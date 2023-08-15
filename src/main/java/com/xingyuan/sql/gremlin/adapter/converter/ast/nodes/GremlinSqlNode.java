package com.xingyuan.sql.gremlin.adapter.converter.ast.nodes;

import com.xingyuan.sql.gremlin.adapter.converter.SqlMetadata;
import lombok.AllArgsConstructor;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This abstract class in the GremlinSql equivalent of SqlNode.
 */
@AllArgsConstructor
public abstract class GremlinSqlNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlNode.class);
    private final SqlNode sqlNode;
    private final SqlMetadata sqlMetadata;
}
