package br.com.emmmanuelneri.repository;

import br.com.emmmanuelneri.domain.Person;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;

import java.util.List;

@AllArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class Repository<T> {

    private final SQLClient client;

    protected void execute(final String query,
                           final Handler<Void> resultHandler,
                           final Handler<Throwable> errorHandler) {
        client.getConnection(connectionHandler -> {
            if (connectionHandler.failed()) {
                errorHandler.handle(connectionHandler.cause());
                return;
            }

            try (final SQLConnection sqlConnection = connectionHandler.result()) {
                sqlConnection.execute(query, updateHandler -> {
                    if (updateHandler.failed()) {
                        errorHandler.handle(updateHandler.cause());
                        return;
                    }

                    resultHandler.handle(updateHandler.result());
                });
            } catch (final Exception ex) {
                errorHandler.handle(ex);
            }
        });
    }

    protected void query(final String query,
                         final Handler<List<T>> resultHandler,
                         final Handler<Throwable> errorHandler) {

        client.getConnection(connectionHandler -> {
            if (connectionHandler.failed()) {
                errorHandler.handle(connectionHandler.cause());
                return;
            }

            try (final SQLConnection sqlConnection = connectionHandler.result()) {
                sqlConnection.query(query, queryHandler -> {
                    if (queryHandler.failed()) {
                        errorHandler.handle(queryHandler.cause());
                        return;
                    }

                    final ResultSet resultSet = queryHandler.result();
                    mapResultSet(resultSet.getResults(), resultHandler, errorHandler);
                });
            } catch (final Exception ex) {
                errorHandler.handle(ex);
            }
        });
    }

    public abstract void findAll(final Handler<List<T>> resultHandler,
                                 final Handler<Throwable> errorHandler);

    public abstract void create(
            final Person person,
            final Handler<Void> resultHandler,
            final Handler<Throwable> errorHandler);

    protected abstract void mapResultSet(final List<JsonArray> results,
                                         final Handler<List<T>> resultHandler,
                                         final Handler<Throwable> errorHandler);
}
