package br.com.emmmanuelneri;

import br.com.emmmanuelneri.domain.Person;
import br.com.emmmanuelneri.infra.DBMigration;
import br.com.emmmanuelneri.infra.SQLConfiguration;
import br.com.emmmanuelneri.repository.PersonRepository;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.SQLClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Application {

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

    public static void main(final String[] args) {
        final Vertx vertx = Vertx.vertx();
        final SQLClient client = SQLConfiguration.create().createSqlClient(vertx);

        final Promise<Void> migrationPromise = Promise.promise();
        final DBMigration migration = DBMigration.create();
        migration.start(client, migrationPromise);

        migrationPromise.future().onComplete(asyncResult -> {
            if (asyncResult.failed()) {
                LOGGER.error("migration error", asyncResult.cause());
                return;
            }

            final PersonRepository personRepository = PersonRepository.create(client);
            final List<Future> insertsPromises = inserts(personRepository, 5);

            CompositeFuture.all(insertsPromises).setHandler(compositeFutureAsyncResult -> {
                if (compositeFutureAsyncResult.failed()) {
                    LOGGER.error("composite error", compositeFutureAsyncResult.cause());
                    return;
                }

                personRepository.findAll(result -> {
                    result.forEach(System.out::println);
                }, error -> LOGGER.error("execute error", error));
            });
        });
    }

    private static List<Future> inserts(final PersonRepository personRepository, final int quantity) {
        final List<Future> promises = new ArrayList<>();

        for (int i = 0; i < quantity; i++) {
            final Promise<Void> promise = Promise.promise();

            final String personName = String.format("Name %d", new Random().nextInt());
            personRepository.create(new Person(personName), result -> promise.complete(), promise::fail);
            promises.add(promise.future());
        }

        return promises;
    }
}
