package com.example.ticketservice.controller;

import com.example.ticketservice.dto.TicketRequest;
import com.example.ticketservice.dto.TicketResponse;
import com.example.ticketservice.model.Ticket;
import com.example.ticketservice.service.TicketService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/tickets")
@RequiredArgsConstructor
@Tag(name = "Ticket Management", description = "Endpoints for managing tickets")
public class TicketController {
    private final TicketService ticketService;

    @PostMapping
    @Operation(summary = "Create a new pending ticket")
    public ResponseEntity<TicketResponse> createTicket(@RequestBody TicketRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(new TicketResponse(ticketService.createPendingTicket(request)));
    }

    @GetMapping("/{ticketId}")
    @Operation(summary = "Get ticket by ID")
    public ResponseEntity<TicketResponse> getTicket(@PathVariable Long ticketId) {
        return ResponseEntity.ok(new TicketResponse(ticketService.findById(ticketId)));
    }

    @GetMapping("/order/{orderId}")
    @Operation(summary = "Get ticket by order ID")
    public ResponseEntity<TicketResponse> getTicketByOrderId(@PathVariable Long orderId) {
        return ResponseEntity.ok(new TicketResponse(ticketService.findByOrderId(orderId)));
    }

    @PutMapping("/{ticketId}/confirm")
    @Operation(summary = "Confirm ticket (manual override)")
    public ResponseEntity<Void> confirmTicket(@PathVariable Long ticketId) {
        ticketService.confirmTicket(ticketId);
        return ResponseEntity.noContent().build();
    }

    @PutMapping("/{ticketId}/cancel")
    @Operation(summary = "Cancel ticket with reason",
            description = "Cancels ticket and initiates refund if paid")
    public ResponseEntity<Void> cancelTicket(
            @PathVariable Long ticketId,
            @RequestParam(required = false) String reason) {
        ticketService.cancelTicket(ticketId,
                reason != null ? reason : "Manual cancellation");
        return ResponseEntity.noContent().build();
    }

    @PutMapping("/{ticketId}/mark-failed")
    @Operation(summary = "Mark ticket as failed (admin only)",
            description = "Forces ticket status to FAILED with provided reason")
    public ResponseEntity<Void> markTicketAsFailed(
            @PathVariable Long ticketId,
            @RequestParam String reason) {
        ticketService.markTicketAsFailed(ticketId, reason);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/{ticketId}/status")
    @Operation(summary = "Get detailed ticket status")
    public ResponseEntity<String> getTicketStatus(@PathVariable Long ticketId) {
        Ticket ticket = ticketService.findById(ticketId);
        String statusMessage = ticket.getStatus() +
                (ticket.getFailureReason() != null ?
                        " (Reason: " + ticket.getFailureReason() + ")" : "");
        return ResponseEntity.ok(statusMessage);
    }
}