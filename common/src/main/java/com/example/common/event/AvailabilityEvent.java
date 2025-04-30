package com.example.common.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AvailabilityEvent {
    private String eventId;
    private String eventType;
    private String attractionId;
    private LocalDateTime visitDate;
    private boolean available;
    private String sagaId;
    private LocalDateTime eventTime;
    private String errorReason;

    public AvailabilityEvent(String eventType, String attractionId,
                             LocalDateTime visitDate, boolean available,
                             String sagaId) {
        this.eventId = UUID.randomUUID().toString();
        this.eventType = eventType;
        this.attractionId = attractionId;
        this.visitDate = visitDate;
        this.available = available;
        this.sagaId = sagaId;
        this.eventTime = LocalDateTime.now();
    }
}