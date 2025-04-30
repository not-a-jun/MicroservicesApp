package com.example.ticketservice.service;

import com.example.common.event.AvailabilityEvent;
import com.example.common.event.OrderEvent;
import com.example.common.event.PaymentEvent;
import com.example.common.event.TicketEvent;
import com.example.common.model.PaymentStatus;
import com.example.common.model.TicketStatus;
import com.example.ticketservice.dto.TicketRequest;
import com.example.ticketservice.exception.TicketNotFoundException;
import com.example.ticketservice.model.Ticket;
import com.example.ticketservice.repository.TicketRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class TicketService {
    private static final String TICKET_CONFIRMED = "TICKET_CONFIRMED";
    private static final String TICKET_CANCELLED = "TICKET_CANCELLED";
    private static final String TICKET_FAILED = "TICKET_FAILED";
    private static final String PAYMENT_REFUND_REQUESTED = "PAYMENT_REFUND_REQUESTED";
    private static final String CHECK_AVAILABILITY = "CHECK_AVAILABILITY";
    private static final String AVAILABILITY_RESPONSE = "AVAILABILITY_RESPONSE";
    private static final String AVAILABILITY_ERROR = "AVAILABILITY_ERROR";
    private static final int MAX_RETRIES = 3;

    private final TicketRepository ticketRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Transactional
    public Ticket createPendingTicket(TicketRequest request) {
        String sagaId = UUID.randomUUID().toString();

        Ticket ticket = Ticket.builder()
                .orderId(request.getOrderId())
                .customerId(request.getCustomerId())
                .attractionId(request.getAttractionId())
                .visitDate(request.getVisitDate())
                .status(TicketStatus.PENDING)
                .bookedAt(LocalDateTime.now())
                .sagaId(sagaId)
                .failureReason(null)
                .build();

        Ticket savedTicket = ticketRepository.save(ticket);
        sendAvailabilityCheckEvent(request, sagaId);
        return savedTicket;
    }

    private void sendAvailabilityCheckEvent(TicketRequest request, String sagaId) {
        AvailabilityEvent event = new AvailabilityEvent(
                CHECK_AVAILABILITY,
                request.getAttractionId(),
                request.getVisitDate(),
                true,
                sagaId
        );
        kafkaTemplate.send("availability-events", event);
        log.info("Sent availability check event for sagaId: {}", sagaId);
    }

    @Transactional
    public void confirmTicket(Long ticketId) {
        Ticket ticket = ticketRepository.findById(ticketId)
                .orElseThrow(() -> new TicketNotFoundException(ticketId));

        if (ticket.getStatus() == TicketStatus.CONFIRMED) {
            log.info("Ticket {} already confirmed", ticketId);
            return;
        }

        ticket.setStatus(TicketStatus.CONFIRMED);
        ticket.setFailureReason(null);
        ticketRepository.save(ticket);

        sendTicketEvent(ticket, TICKET_CONFIRMED, null);
    }

    @Transactional
    @Retryable(maxAttempts = MAX_RETRIES, backoff = @Backoff(delay = 1000))
    public void cancelTicket(Long ticketId, String reason) {
        Ticket ticket = ticketRepository.findById(ticketId)
                .orElseThrow(() -> new TicketNotFoundException(ticketId));

        if (ticket.getStatus() != TicketStatus.CANCELLED &&
                ticket.getStatus() != TicketStatus.FAILED) {

            ticket.setStatus(TicketStatus.CANCELLED);
            ticket.setFailureReason(reason);
            ticketRepository.save(ticket);

            sendTicketEvent(ticket, TICKET_CANCELLED, reason);

            if (ticket.getStatus() == TicketStatus.CONFIRMED) {
                requestPaymentRefund(ticket, reason);
            }
        }
    }

    @Transactional
    public void markTicketAsFailed(Long ticketId, String failureReason) {
        Ticket ticket = ticketRepository.findById(ticketId)
                .orElseThrow(() -> new TicketNotFoundException(ticketId));

        ticket.setStatus(TicketStatus.FAILED);
        ticket.setFailureReason(failureReason);
        ticketRepository.save(ticket);

        sendTicketEvent(ticket, TICKET_FAILED, failureReason);
        log.error("Ticket {} marked as failed. Reason: {}", ticketId, failureReason);
    }

    private void sendTicketEvent(Ticket ticket, String eventType, String reason) {
        TicketEvent event = new TicketEvent(
                eventType,
                ticket.getId(),
                ticket.getOrderId(),
                ticket.getCustomerId(),
                ticket.getAttractionId(),
                ticket.getVisitDate(),
                ticket.getStatus(),
                ticket.getSagaId(),
                reason
        );
        kafkaTemplate.send("ticket-events", event);
    }

    private void requestPaymentRefund(Ticket ticket, String reason) {
        PaymentEvent refundEvent = PaymentEvent.builder()
                .eventType(PAYMENT_REFUND_REQUESTED)
                .orderId(ticket.getOrderId())
                .customerId(ticket.getCustomerId())
                .amount(BigDecimal.valueOf(100.00))
                .status(PaymentStatus.REFUND_PENDING)
                .sagaId(ticket.getSagaId())
                .build();
        kafkaTemplate.send("payment-refund-events", refundEvent);
    }

    @KafkaListener(topics = "availability-events")
    public void handleAvailabilityEvent(AvailabilityEvent event) {
        try {
            if ("AVAILABILITY_RESPONSE".equals(event.getEventType())) {
                ticketRepository.findBySagaId(event.getSagaId())
                        .ifPresent(ticket -> {
                            if (event.isAvailable()) {
                                updateTicketStatus(ticket, TicketStatus.AVAILABLE);
                                initiatePaymentProcess(ticket);
                            } else {
                                cancelTicket(ticket.getId(), "Attraction not available");
                            }
                        });
            } else if ("AVAILABILITY_ERROR".equals(event.getEventType())) {
                handleAvailabilityError(event);
            }
        } catch (Exception e) {
            log.error("Error processing availability event", e);
        }
    }

    private void handleAvailabilityError(AvailabilityEvent event) {
        ticketRepository.findBySagaId(event.getSagaId())
                .ifPresent(ticket -> {
                    markTicketAsFailed(
                            ticket.getId(),
                            "Availability check failed: " + event.getErrorReason()
                    );
                });
    }

    private void processAvailabilityResponse(AvailabilityEvent event) {
        ticketRepository.findBySagaId(event.getSagaId())
                .ifPresentOrElse(ticket -> {
                    if (event.isAvailable()) {
                        updateTicketStatus(ticket, TicketStatus.AVAILABLE);
                        initiatePaymentProcess(ticket);
                    } else {
                        cancelTicket(ticket.getId(), "Attraction not available");
                    }
                }, () -> log.error("Ticket not found for sagaId: {}", event.getSagaId()));
    }

    private void updateTicketStatus(Ticket ticket, TicketStatus status) {
        ticket.setStatus(status);
        ticket.setFailureReason(null);
        ticketRepository.save(ticket);
        log.info("Ticket {} status updated to {}", ticket.getId(), status);
    }

    private void initiatePaymentProcess(Ticket ticket) {
        PaymentEvent paymentEvent = PaymentEvent.builder()
                .eventType("PROCESS_PAYMENT")
                .orderId(ticket.getOrderId())
                .customerId(ticket.getCustomerId())
                .amount(calculateTicketPrice(ticket))
                .status(PaymentStatus.PENDING)
                .sagaId(ticket.getSagaId())
                .build();

        kafkaTemplate.send("payment-events", paymentEvent);
        log.info("Payment initiated for ticket {}", ticket.getId());
    }

    private BigDecimal calculateTicketPrice(Ticket ticket) {
        // Заглушка - реализовать логику расчета цены
        return BigDecimal.valueOf(100.00);
    }

    private void processAvailabilityError(AvailabilityEvent event) {
        ticketRepository.findBySagaId(event.getSagaId())
                .ifPresent(ticket -> {
                    markTicketAsFailed(ticket.getId(),
                            "Availability check error: " + event.getErrorReason());
                });
    }

    @KafkaListener(topics = "payment-events")
    public void handlePaymentEvent(PaymentEvent event) {
        try {
            log.info("Processing payment event: {}", event);

            ticketRepository.findBySagaId(event.getSagaId())
                    .ifPresentOrElse(ticket -> {
                        switch (event.getStatus()) {
                            case COMPLETED:
                                handleSuccessfulPayment(ticket, event);
                                break;

                            case FAILED:
                                handleFailedPayment(ticket, event);
                                break;

                            case REFUND_PENDING:
                                log.info("Refund initiated for ticket {}", ticket.getId());
                                break;

                            case REFUNDED:
                                handleRefundCompleted(ticket, event);
                                break;

                            case REFUND_FAILED:
                                handleRefundFailed(ticket, event);
                                break;

                            default:
                                log.warn("Unknown payment status: {}", event.getStatus());
                        }
                    }, () -> log.error("Ticket not found for sagaId: {}", event.getSagaId()));

        } catch (Exception e) {
            log.error("Critical error processing payment event: {}", event, e);
            // Можно добавить отправку в Dead Letter Queue
        }
    }

    private void handleSuccessfulPayment(Ticket ticket, PaymentEvent event) {
        if (ticket.getStatus() != TicketStatus.CONFIRMED) {
            ticket.setStatus(TicketStatus.CONFIRMED);
            ticket.setFailureReason(null);
            ticketRepository.save(ticket);

            sendTicketEvent(ticket, "TICKET_CONFIRMED", null);
            log.info("Ticket {} confirmed successfully", ticket.getId());
        }
    }

    private void handleFailedPayment(Ticket ticket, PaymentEvent event) {
        if (event.getRetryCount() < MAX_RETRIES) {
            // Повторяем попытку платежа
            PaymentEvent retryEvent = event.toBuilder()
                    .retryCount(event.getRetryCount() + 1)
                    .build();

            kafkaTemplate.send("payment-retry-events", retryEvent);
            log.warn("Payment failed, retrying (attempt {})", retryEvent.getRetryCount());
        } else {
            // Максимальное количество попыток исчерпано
            ticket.setStatus(TicketStatus.FAILED);
            ticket.setFailureReason("Payment failed after " + MAX_RETRIES + " attempts");
            ticketRepository.save(ticket);

            sendTicketEvent(ticket, "TICKET_FAILED", ticket.getFailureReason());
            log.error("Ticket {} failed after payment retries", ticket.getId());

            // Инициируем компенсирующие транзакции
            initiateCompensation(ticket, "payment-failure");
        }
    }

    private void handleRefundCompleted(Ticket ticket, PaymentEvent event) {
        ticket.setStatus(TicketStatus.CANCELLED);
        ticket.setFailureReason("Payment refund completed");
        ticketRepository.save(ticket);

        sendTicketEvent(ticket, "TICKET_CANCELLED", "Refund completed");
        log.info("Ticket {} cancelled after successful refund", ticket.getId());
    }

    private void handleRefundFailed(Ticket ticket, PaymentEvent event) {
        ticket.setStatus(TicketStatus.FAILED);
        ticket.setFailureReason("Refund failed: " + event.getFailureReason());
        ticketRepository.save(ticket);

        sendTicketEvent(ticket, "TICKET_FAILED", ticket.getFailureReason());
        log.error("Refund failed for ticket {}", ticket.getId());

        // Можно добавить логику для ручного вмешательства
        sendAdminNotification(ticket, "refund-failed");
    }

    private void initiateCompensation(Ticket ticket, String reason) {
        // 1. Отмена бронирования в ticket-service
        sendTicketEvent(ticket, "TICKET_CANCELLED", reason);

        // 2. Если нужно - отмена других связанных операций
        if (ticket.getOrderId() != null) {
            OrderEvent orderEvent = OrderEvent.builder()
                    .orderId(ticket.getOrderId())
                    .eventType("ORDER_CANCELLED: Payment failed for ticket " + ticket.getId())
                    .customerId("Payment failed for ticket " + ticket.getId())
                    .build();
            kafkaTemplate.send("order-compensation-events", orderEvent);
        }

        log.warn("Initiated compensation for ticket {}", ticket.getId());
    }

    private void sendAdminNotification(Ticket ticket, String errorType) {
        // Реализация отправки уведомления администратору
        // Можно использовать отдельный топик Kafka или email/sms
        log.warn("Admin notification sent for {} (ticket {})",
                errorType, ticket.getId());
    }

    public Ticket findById(Long ticketId) {
        return ticketRepository.findById(ticketId)
                .orElseThrow(() -> new TicketNotFoundException(ticketId));
    }

    public Ticket findByOrderId(Long orderId) {
        return ticketRepository.findByOrderId(orderId)
                .orElseThrow(() -> new TicketNotFoundException(orderId));
    }
}