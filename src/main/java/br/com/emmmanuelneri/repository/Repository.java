package br.com.emmmanuelneri.repository;

import br.com.emmmanuelneri.domain.Person;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;

import java.util.List;
import java.util.stream.Collectors;

public abstract class Repository<T> {

    private final SQLClient client;

    protected Repository(SQLClient client) {
        this.client = client;
    }

    protected void execute(final String query,
                           final Handler<Void> resultHandler,
                           final Handler<Throwable> errorHandler) {
        client.getConnection(connectionHandler -> {
            if (connectionHandler.failed()) {
                errorHandler.handle(connectionHandler.cause());
                return;
            }

            try (final SQLConnection sqlConnection = connectionHandler.result()) {
                final Promise<Void> promise = Promise.promise();

                execute(query, sqlConnection, promise);
                promise.future().setHandler(asyncResult -> {
                    if (asyncResult.failed()) {
                        errorHandler.handle(asyncResult.cause());
                        return;
                    }

                    resultHandler.handle(asyncResult.result());
                });
            } catch (final Exception ex) {
                errorHandler.handle(ex);
            }
        });
    }

    private void execute(final String query, final SQLConnection sqlConnection, final Promise<Void> promise) {
        sqlConnection.execute(query, executeHandler -> {
            if (executeHandler.failed()) {
                promise.fail(executeHandler.cause());
                return;
            }

            promise.complete(executeHandler.result());
        });
    }

    protected void executeInTransaction(final List<String> queries,
                                        final Handler<Void> resultHandler,
                                        final Handler<Throwable> errorHandler) {
        client.getConnection(connectionHandler -> {
            if (connectionHandler.failed()) {
                errorHandler.handle(connectionHandler.cause());
                return;
            }

            final SQLConnection sqlConnection = connectionHandler.result();
            sqlConnection.setAutoCommit(false, autoCommitResult -> {
                if (autoCommitResult.failed()) {
                    errorHandler.handle(autoCommitResult.cause());
                    return;
                }

                final List<Future> promises = queries.stream()
                        .map(query -> {
                            final Promise<Void> promise = Promise.promise();
                            execute(query, sqlConnection, promise);
                            return promise.future();
                        }).collect(Collectors.toList());

                CompositeFuture.all(promises).setHandler(compositeFutureAsyncResult -> {
                    if (compositeFutureAsyncResult.failed()) {
                        sqlConnection.rollback(asyncResult -> {
                        });
                        errorHandler.handle(compositeFutureAsyncResult.cause());
                        return;
                    }

                    sqlConnection.commit(commitResult -> {
                        if (commitResult.failed()) {
                            errorHandler.handle(commitResult.cause());
                            return;
                        }

                        resultHandler.handle(commitResult.result());
                        sqlConnection.close();
                    });
                });
            });
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
