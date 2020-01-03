package br.com.emmmanuelneri;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;

import java.util.List;

public class Application {

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

    public static void main(final String[] args) {
        final Vertx vertx = Vertx.vertx();

        final JsonObject config = new JsonObject()
                .put("url", "jdbc:ignite:thin://localhost:10800")
                .put("max_pool_size", 30);

        final SQLClient client = JDBCClient.createShared(vertx, config);

        execute(client, "create table if not exists Person (" +
                " id UUID, name VARCHAR," +
                " PRIMARY KEY (id))" +
                " WITH \"template=replicated\""
        );

        execute(client, "INSERT INTO Person (id, name) values (RANDOM_UUID(), 'Name 1');");
        execute(client, "INSERT INTO Person (id, name) values (RANDOM_UUID(), 'Name 2');");
        execute(client, "INSERT INTO Person (id, name) values (RANDOM_UUID(), 'Name 3');");
        execute(client, "INSERT INTO Person (id, name) values (RANDOM_UUID(), 'Name 4');");
        execute(client, "INSERT INTO Person (id, name) values (RANDOM_UUID(), 'Name 5');");

        query(client, "SELECT * FROM Person", result -> result.forEach(person -> LOGGER.info("value: {0}", person)));
    }

    private static void execute(final SQLClient client, final String query) {
        client.getConnection(connectionHandler -> {
            if (connectionHandler.failed()) {
                LOGGER.error("connection error", connectionHandler.cause());
                return;
            }

            try (final SQLConnection sqlConnection = connectionHandler.result()) {
                sqlConnection.execute(query, sqlHandler -> {
                    if (sqlHandler.failed()) {
                        LOGGER.error("query error", sqlHandler.cause());
                        return;
                    }

                    LOGGER.info("executed");
                });
            } catch (final Exception ex) {
                LOGGER.error("connectiom error", ex);
            }
        });
    }

    private static void query(final SQLClient client, final String query, final Handler<List<JsonArray>> resultList) {
        client.getConnection(connectionHandler -> {
            if (connectionHandler.failed()) {
                LOGGER.error("connection error", connectionHandler.cause());
                return;
            }

            try (final SQLConnection sqlConnection = connectionHandler.result()) {
                sqlConnection.query(query, queryHandler -> {
                    if (queryHandler.failed()) {
                        LOGGER.error("query error", queryHandler.cause());
                        return;
                    }

                    final ResultSet resultSet = queryHandler.result();
                    resultList.handle(resultSet.getResults());
                });
            } catch (final Exception ex) {
                LOGGER.error("connectiom error", ex);
            }
        });
    }
}
