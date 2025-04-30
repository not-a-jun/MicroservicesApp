package com.example.ticketservice.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.NOT_FOUND)
public class TicketNotFoundException extends RuntimeException {

    public TicketNotFoundException(Long ticketId) {
        super(String.format("Ticket not found with ID: %d", ticketId));
    }

    public TicketNotFoundException(String orderId) {
        super(String.format("Ticket not found with order ID: %s", orderId));
    }
}
