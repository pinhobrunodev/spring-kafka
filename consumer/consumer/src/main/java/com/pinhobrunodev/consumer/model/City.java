package com.pinhobrunodev.consumer.model;

import lombok.*;

import java.io.Serializable;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class City implements Serializable {

    private String name;
    private String UF;
}
