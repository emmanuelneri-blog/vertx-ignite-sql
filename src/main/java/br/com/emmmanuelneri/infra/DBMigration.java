package br.com.emmmanuelneri.infra;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Promise;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import lombok.NoArgsConstructor;

import java.util.Arrays;

@NoArgsConstructor(staticName = "create")
public final class DBMigration {

    private static final String ADDRESS_TABLE = "create table if not exists Address (" +
            " id UUID," +
            " street VARCHAR," +
            " number INT," +
            " PRIMARY KEY (id))" +
            " WITH \"template=replicated,ATOMICITY=TRANSACTIONAL_SNAPSHOT\"";

    private static final String PERSON_TABLE = "create table if not exists Person (" +
            " id UUID," +
            " name VARCHAR," +
            " address_id UUID," +
            " PRIMARY KEY (id, address_id))" +
            " WITH \"template=replicated,ATOMICITY=TRANSACTIONAL_SNAPSHOT,affinity_key=address_id\"";

    public void start(final SQLClient client, final Promise<Void> promise) {
        final Promise<Void> personTableCreationPromise = Promise.promise();
        final Promise<Void> addressTableCreationPromise = Promise.promise();

        execute(client, PERSON_TABLE, personTableCreationPromise);
        execute(client, ADDRESS_TABLE, addressTableCreationPromise);

        CompositeFuture.all(Arrays.asList(personTableCreationPromise.future(), addressTableCreationPromise.future()))
                .setHandler(asyncResult -> {
                    if (asyncResult.failed()) {
                        promise.fail(asyncResult.cause());
                        return;
                    }

                    promise.complete();
                });
    }

    private void execute(final SQLClient client,
                         final String query,
                         final Promise<Void> promise) {
        client.getConnection(connectionHandler -> {
            if (connectionHandler.failed()) {
                promise.fail(connectionHandler.cause());
                return;
            }

            try (final SQLConnection sqlConnection = connectionHandler.result()) {
                sqlConnection.execute(query, sqlHandler -> {
                    if (sqlHandler.failed()) {
                        promise.fail(sqlHandler.cause());
                        return;
                    }

                    promise.complete();
                });
            } catch (final Exception ex) {
                promise.fail(ex);
            }
        });
    }
}
