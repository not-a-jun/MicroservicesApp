package com.example.ticketservice.dto;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class TicketRequest {
    private Long orderId;
    private String customerId;
    private String attractionId;
    private LocalDateTime visitDate;
}
