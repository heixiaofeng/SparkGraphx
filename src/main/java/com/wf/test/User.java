package com.wf.test;


import java.io.Serializable;

public class User implements Serializable {
    private String name;
    public User(String name){
        super();
        this.name = name;
    }

    public void setName(String name) {
        this.name = name;
    }
    public String getName(){
        return name;
    }
}
