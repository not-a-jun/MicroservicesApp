package com.example.ticketservice.model;

import com.example.common.model.TicketStatus;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Data
@Builder
@Table(name = "tickets")
@NoArgsConstructor
@AllArgsConstructor
public class Ticket {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long orderId;
    private String customerId;
    private String attractionId;
    private LocalDateTime visitDate;

    @Enumerated(EnumType.STRING)
    private TicketStatus status;

    private LocalDateTime bookedAt;
    private String sagaId;
    private String failureReason;
}
