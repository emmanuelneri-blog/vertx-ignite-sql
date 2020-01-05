package br.com.emmmanuelneri.infra;

import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import lombok.NoArgsConstructor;

@NoArgsConstructor(staticName = "create")
public final class DBMigration {

    private static final String PERSON_TABLE = "create table if not exists Person (" +
            " id UUID, name VARCHAR," +
            " PRIMARY KEY (id))" +
            " WITH \"template=replicated\"";

    public void start(final SQLClient client, final Promise<Void> promise) {
        execute(client, PERSON_TABLE, resultHandler -> promise.complete(), promise::fail);
    }

    private void execute(final SQLClient client, final String query,
                         final Handler<Void> resultHandler,
                         final Handler<Throwable> errorHandler) {
        client.getConnection(connectionHandler -> {
            if (connectionHandler.failed()) {
                errorHandler.handle(connectionHandler.cause());
                return;
            }

            try (final SQLConnection sqlConnection = connectionHandler.result()) {
                sqlConnection.execute(query, sqlHandler -> {
                    if (sqlHandler.failed()) {
                        errorHandler.handle(sqlHandler.cause());
                        return;
                    }

                    resultHandler.handle(null);
                });
            } catch (final Exception ex) {
                errorHandler.handle(ex);
            }
        });
    }
}
