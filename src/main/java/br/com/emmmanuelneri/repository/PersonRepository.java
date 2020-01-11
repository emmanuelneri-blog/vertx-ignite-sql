package br.com.emmmanuelneri.repository;

import br.com.emmmanuelneri.domain.Address;
import br.com.emmmanuelneri.domain.Person;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLClient;

import java.util.Arrays;
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
        final String query = "SELECT p.id, p.name, a.id, a.street, a.number" +
                " FROM Person as p" +
                " INNER JOIN Address as a on a.id = p.address_id;";
        query(query, resultHandler, errorHandler);
    }

    @Override
    public void create(final Person person,
                       final Handler<Void> resultHandler,
                       final Handler<Throwable> errorHandler) {

        final Address address = person.getAddress();
        address.setId(UUID.randomUUID());

        final String addressSQL = String.format("INSERT INTO Address (id, street, number) values ('%s', '%s', '%d');",
                address.getId(), address.getStreet(), address.getNumber());

        final String personSQL = String.format("INSERT INTO Person (id, name, address_id) values (RANDOM_UUID(), '%s', '%s');",
                person.getName(), address.getId().toString());

        executeInTransaction(Arrays.asList(addressSQL, personSQL), resultHandler, errorHandler);
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

                        final Address address = new Address();
                        address.setId(UUID.fromString(result.getString(2)));
                        address.setStreet(result.getString(3));
                        address.setNumber(result.getInteger(4));

                        person.setAddress(address);
                        return person;
                    }).collect(Collectors.toList());
            resultHandler.handle(persons);
        } catch (Exception ex) {
            errorHandler.handle(ex);
        }
    }
}
