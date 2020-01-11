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
public class Address {

    private UUID id;
    private String street;
    private int number;

    public Address(final String street, final int number) {
        this.street = street;
        this.number = number;
    }
}
