package com.example.ticketservice.dto;

import com.example.common.model.TicketStatus;
import com.example.ticketservice.model.Ticket;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public class TicketResponse {
    private Long id;
    private Long orderId;
    private String customerId;
    private String attractionId;
    private LocalDateTime visitDate;
    private TicketStatus status;
    private LocalDateTime bookedAt;
    private String failureReason;

    public TicketResponse(Ticket ticket) {
        this.id = ticket.getId();
        this.orderId = ticket.getOrderId();
        this.customerId = ticket.getCustomerId();
        this.attractionId = ticket.getAttractionId();
        this.visitDate = ticket.getVisitDate();
        this.status = ticket.getStatus();
        this.bookedAt = ticket.getBookedAt();
        this.failureReason = ticket.getFailureReason();
    }
}
