package com.example.common.event;

import com.example.common.model.TicketStatus;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TicketEvent {
    private String eventId;
    private String eventType;
    private Long ticketId;
    private Long orderId;
    private String customerId;
    private String attractionId;
    private LocalDateTime visitDate;
    private TicketStatus status;
    private LocalDateTime eventTime;
    private String sagaId;
    private String compensationReason;
    private String failureReason;

    public TicketEvent(String eventType, Long ticketId, Long orderId,
                       String customerId, String attractionId,
                       LocalDateTime visitDate, TicketStatus status, String sagaId, String compensationReason) {
        this.eventId = UUID.randomUUID().toString();
        this.eventType = eventType;
        this.ticketId = ticketId;
        this.orderId = orderId;
        this.customerId = customerId;
        this.attractionId = attractionId;
        this.visitDate = visitDate;
        this.status = status;
        this.eventTime = LocalDateTime.now();
        this.sagaId = sagaId;
        this.compensationReason = compensationReason;
    }
}
