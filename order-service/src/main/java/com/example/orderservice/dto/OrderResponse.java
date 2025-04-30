package com.example.orderservice.dto;

import com.example.common.model.OrderStatus;
import com.example.orderservice.model.Order;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
public class OrderResponse {
    private Long id;
    private String customerId;
    private String description;
    private BigDecimal amount;
    private OrderStatus status;
    private LocalDateTime createdAt;

    public OrderResponse(Order order) {
        this.id = order.getId();
        this.customerId = order.getCustomerId();
        this.description = order.getDescription();
        this.amount = order.getAmount();
        this.status = order.getStatus();
        this.createdAt = order.getCreatedAt();
    }
}
