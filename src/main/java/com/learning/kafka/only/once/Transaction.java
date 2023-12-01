/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Record.java to edit this template
 */
package com.learning.kafka.only.once;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.security.InvalidParameterException;
import org.slf4j.LoggerFactory;

/**
 *
 * @author caio
 */
public record Transaction(String name, Float amount, String timestamp) {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(Transaction.class.getSimpleName());

    @JsonIgnore
    public Transaction merge(Transaction newTransaction){
        if((this.name!=null && (!this.name.equals(newTransaction.name)))  || newTransaction.name==null){
            throw new RuntimeException("invalid operation", new InvalidParameterException(name));
        }
        return new Transaction(newTransaction.name, this.amount+newTransaction.amount, newTransaction.timestamp);
    }
    
    @JsonIgnore
    public String toString(){
        try {
            ObjectMapper om = new ObjectMapper();
            String transaction = om.writeValueAsString(this);
            return transaction;
        } catch (JsonProcessingException ex) {
            log.error("unable to serialize transaction", ex);
            return null;
        }
    }
}
