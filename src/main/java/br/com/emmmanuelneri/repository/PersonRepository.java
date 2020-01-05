package br.com.emmmanuelneri.repository;

import br.com.emmmanuelneri.domain.Person;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLClient;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class PersonRepository extends Repository<Person> {

    private PersonRepository(SQLClient client) {
        super(client);
    }

    public static PersonRepository create(final SQLClient client) {
        return new PersonRepository(client);
    }

    @Override
    public void findAll(final Handler<List<Person>> resultHandler,
                        final Handler<Throwable> errorHandler) {
        query("SELECT id,name FROM Person;", resultHandler, errorHandler);
    }

    @Override
    public void create(final Person person,
                       final Handler<Void> resultHandler,
                       final Handler<Throwable> errorHandler) {
        final String sql = String.format("INSERT INTO Person (id, name) values (RANDOM_UUID(), '%s');", person.getName());
        execute(sql, resultHandler, errorHandler);
    }

    @Override
    protected void mapResultSet(final List<JsonArray> results,
                                final Handler<List<Person>> resultHandler,
                                final Handler<Throwable> errorHandler) {

        try {
            final List<Person> persons = results.stream()
                    .map(result -> {
                        final Person person = new Person();
                        person.setId(UUID.fromString(result.getString(0)));
                        person.setName(result.getString(1));
                        return person;
                    }).collect(Collectors.toList());
            resultHandler.handle(persons);
        } catch (Exception ex) {
            errorHandler.handle(ex);
        }
    }
}
