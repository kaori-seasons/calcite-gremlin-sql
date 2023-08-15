package com.xingyuan.sql.gremlin.adapter.converter;

import com.google.common.collect.ImmutableList;
import com.xingyuan.sql.gremlin.adapter.converter.ast.nodes.GremlinSqlFactory;
import com.xingyuan.sql.gremlin.adapter.converter.ast.nodes.select.GremlinSqlSelect;
import com.xingyuan.sql.gremlin.adapter.converter.schema.calcite.GremlinSchema;
import com.xingyuan.sql.gremlin.adapter.results.SqlGremlinQueryResult;
import lombok.Getter;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.SQLException;
import java.util.List;

/**
 * This module is the entry point of the SqlGremlin conversion.
 */
public class SqlConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlConverter.class);
    private static final List<RelTraitDef> TRAIT_DEFS =
            ImmutableList.of(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE);
    private static final SqlParser.Config PARSER_CONFIG = SqlParser.configBuilder().setLex(Lex.MYSQL).setQuoting(
            Quoting.DOUBLE_QUOTE).build();
    private static final Program PROGRAM =
            Programs.sequence(Programs.ofRules(Programs.RULE_SET), Programs.CALC_PROGRAM);
    private final FrameworkConfig frameworkConfig;
    private final GraphTraversalSource g;
    private final GremlinSchema gremlinSchema;


    public SqlConverter(final GremlinSchema gremlinSchema, final GraphTraversalSource g) {
        this.gremlinSchema = gremlinSchema;
        final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        this.frameworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(PARSER_CONFIG)
                .defaultSchema(rootSchema.add("gremlin", gremlinSchema))
                .traitDefs(TRAIT_DEFS)
                .programs(PROGRAM)
                .build();
        this.g = g;
    }

    // NOT THREAD SAFE
    public SqlGremlinQueryResult executeQuery(final String query) throws SQLException {
        final SqlMetadata sqlMetadata = new SqlMetadata(g, gremlinSchema);
        GremlinSqlFactory.setSqlMetadata(sqlMetadata);
        // Not sure if this can be re-used?
        final QueryPlanner queryPlanner = new QueryPlanner(frameworkConfig);

        queryPlanner.plan(query);
        final SqlNode sqlNode = queryPlanner.getValidate();

        if (sqlNode instanceof SqlSelect) {
            final GremlinSqlSelect gremlinSqlSelect = GremlinSqlFactory.createSelect((SqlSelect) sqlNode, g);
            return gremlinSqlSelect.executeTraversal();
        } else {
            throw new SQLException("Only sql select statements are supported right now.");
        }
    }

    private GraphTraversal<?, ?> getGraphTraversal(final String query) throws SQLException {
        final SqlMetadata sqlMetadata = new SqlMetadata(g, gremlinSchema);
        GremlinSqlFactory.setSqlMetadata(sqlMetadata);
        // Not sure if this can be re-used?
        final QueryPlanner queryPlanner = new QueryPlanner(frameworkConfig);

        queryPlanner.plan(query);
        final SqlNode sqlNode = queryPlanner.getValidate();

        if (sqlNode instanceof SqlSelect) {
            final GremlinSqlSelect gremlinSqlSelect = GremlinSqlFactory.createSelect((SqlSelect) sqlNode, g);
            return gremlinSqlSelect.generateTraversal();
        } else {
            throw new SQLException("Only sql select statements are supported right now.");
        }
    }

    public String getStringTraversal(final String query) throws SQLException {
        return GroovyTranslator.of("g").translate(getGraphTraversal(query).asAdmin().getBytecode());
    }

    @Getter
    private static class QueryPlanner {
        private final Planner planner;
        private SqlNode validate;

        QueryPlanner(final FrameworkConfig frameworkConfig) {
            this.planner = Frameworks.getPlanner(frameworkConfig);
        }

        public void plan(final String sql) throws SQLException {
            try {
                validate = planner.validate(planner.parse(sql));
            } catch (final Exception e) {
                throw new SQLException(String.format("Error parsing: \"%s\". Error: \"%s\".", sql, e), e);
            }
        }
    }
}
