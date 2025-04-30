package com.example.orderservice.dto;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class OrderRequest {
    private String customerId;
    private String description;
    private BigDecimal amount;
}
