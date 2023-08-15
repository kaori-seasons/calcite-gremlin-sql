package com.xingyuan.sql.gremlin.adapter.results.pagination;

import java.util.Map;

interface GetRowFromMap {
    Object[] execute(Map<String, Object> input);
}
