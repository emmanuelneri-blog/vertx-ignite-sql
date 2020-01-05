package br.com.emmmanuelneri.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.UUID;

@Getter
@Setter
@ToString
@NoArgsConstructor
public class Person {

    private UUID id;
    private String name;

    public Person(final String name) {
        this.name = name;
    }
}
