package com.xingyuan.sql.gremlin.adapter;

import com.xingyuan.sql.gremlin.adapter.converter.SqlConverter;
import com.xingyuan.sql.gremlin.adapter.converter.schema.SqlSchemaGrabber;
import com.xingyuan.sql.gremlin.adapter.converter.schema.calcite.GremlinSchema;
import com.xingyuan.sql.gremlin.adapter.results.SqlGremlinQueryResult;
import lombok.Getter;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.jupiter.api.Assertions;


import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class GremlinSqlBaseTest {
    private final Graph graph;
    private final GraphTraversalSource g;
    private final SqlConverter converter;

    GremlinSqlBaseTest() throws SQLException {
        graph = TestGraphFactory.createGraph(getDataSet());
        g = graph.traversal();
        final GremlinSchema gremlinSchema = SqlSchemaGrabber.getSchema(g, SqlSchemaGrabber.ScanType.All);
        converter = new SqlConverter(gremlinSchema, g);
    }

    protected abstract DataSet getDataSet();

    protected void runQueryTestResults(final String query, final List<String> columnNames,
                                       final List<List<Object>> rows)
            throws SQLException {
        final SqlGremlinTestResult result = new SqlGremlinTestResult(converter.executeQuery(query));
        assertRows(result.getRows(), rows);
        assertColumns(result.getColumns(), columnNames);
    }

    public List<List<Object>> rows(final List<Object>... rows) {
        return new ArrayList<>(Arrays.asList(rows));
    }

    public List<String> columns(final String... columns) {
        return new ArrayList<>(Arrays.asList(columns));
    }

    public List<Object> r(final Object... row) {
        return new ArrayList<>(Arrays.asList(row));
    }

    public void assertRows(final List<List<Object>> actual, final List<List<Object>> expected) {
        Assertions.assertEquals(expected.size(), actual.size());
        for (int i = 0; i < actual.size(); i++) {
            Assertions.assertEquals(expected.get(i).size(), actual.get(i).size());
            for (int j = 0; j < actual.get(i).size(); j++) {
                Assertions.assertEquals(expected.get(i).get(j), actual.get(i).get(j));
            }
        }
    }

    public void assertColumns(final List<String> actual, final List<String> expected) {
        Assertions.assertEquals(expected.size(), actual.size());
        for (int i = 0; i < actual.size(); i++) {
            Assertions.assertEquals(expected.get(i), actual.get(i));
        }
    }

    public enum DataSet {
        SPACE,
        DATA_TYPES
    }

    @Getter
    static class SqlGremlinTestResult {
        private final List<List<Object>> rows = new ArrayList<>();
        private final List<String> columns;

        SqlGremlinTestResult(final SqlGremlinQueryResult sqlGremlinQueryResult) throws SQLException {
            columns = sqlGremlinQueryResult.getColumns();
            Object res;
            do {
                try {
                    res = sqlGremlinQueryResult.getResult();
                } catch (final SQLException e) {
                    if (e.getMessage().equals(SqlGremlinQueryResult.EMPTY_MESSAGE)) {
                        break;
                    } else {
                        throw e;
                    }
                }
                this.rows.add((List<Object>) res);
            } while (true);
        }
    }
}
