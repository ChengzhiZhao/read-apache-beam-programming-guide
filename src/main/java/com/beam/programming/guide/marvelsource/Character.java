package com.beam.programming.guide.marvelsource;

import java.io.Serializable;

public class Character implements Serializable {
    private int id;
    private int power;
    private String name;

    public int getId() {
        return id;
    }

    public int getPower() {
        return power;
    }

    public String getName() {
        return name;
    }

    public Character(int id, int power, String name) {
        this.id = id;
        this.power = power;
        this.name = name;
    }
}
