package br.com.emmmanuelneri.infra;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import lombok.NoArgsConstructor;

@NoArgsConstructor(staticName = "create")
public final class SQLConfiguration {

    public SQLClient createSqlClient(final Vertx vertx) {
        final JsonObject config = new JsonObject()
                .put("url", "jdbc:ignite:thin://localhost:10800")
                .put("max_pool_size", 30);

        return JDBCClient.createShared(vertx, config);
    }
}
