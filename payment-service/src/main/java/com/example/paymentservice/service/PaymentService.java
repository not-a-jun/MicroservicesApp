package com.example.paymentservice.service;

import com.example.common.event.OrderEvent;
import com.example.common.event.PaymentEvent;
import com.example.common.model.PaymentStatus;
import com.example.paymentservice.dto.PaymentDetailsResponse;
import com.example.paymentservice.dto.PaymentProcessRequest;
import com.example.paymentservice.dto.PaymentStatusResponse;
import com.example.paymentservice.dto.RefundRequest;
import com.example.paymentservice.exception.PaymentNotFoundException;
import com.example.paymentservice.exception.PaymentProcessingException;
import com.example.paymentservice.external.ExternalPaymentGateway;
import com.example.paymentservice.model.Payment;
import com.example.paymentservice.repository.PaymentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final KafkaTemplate<String, PaymentEvent> kafkaTemplate;
    private final ExternalPaymentGateway paymentGateway;

    @Transactional
    @KafkaListener(topics = "${kafka.topics.order-events}", groupId = "${spring.kafka.consumer.group-id}")
    public void processPayment(OrderEvent orderEvent) {
        if (!"ORDER_CREATED".equals(orderEvent.getEventType())) {
            return;
        }

        log.info("Processing payment for order {}", orderEvent.getOrderId());

        try {
            Payment payment = createPaymentFromOrder(orderEvent);
            processPaymentWithGateway(payment);
            handleSuccessfulPayment(payment);
        } catch (Exception ex) {
            handlePaymentFailure(orderEvent, ex);
        }
    }

    private Payment createPaymentFromOrder(OrderEvent orderEvent) {
        Payment payment = new Payment();
        payment.setOrderId(orderEvent.getOrderId());
        payment.setCustomerId(orderEvent.getCustomerId());
        payment.setAmount(orderEvent.getAmount());
        payment.setProcessedAt(LocalDateTime.now());
        payment.setStatus(PaymentStatus.PENDING);
        return paymentRepository.save(payment);
    }

    @Retryable(maxAttempts = 3, backoff = @Backoff(delay = 1000))
    private void processPaymentWithGateway(Payment payment) {
        boolean paymentSuccess = paymentGateway.processPayment(
                payment.getCustomerId(),
                payment.getAmount()
        );

        if (!paymentSuccess) {
            throw new PaymentProcessingException("Payment gateway declined transaction");
        }
    }

    private void handleSuccessfulPayment(Payment payment) {
        payment.setStatus(PaymentStatus.COMPLETED);
        Payment savedPayment = paymentRepository.save(payment);

        PaymentEvent successEvent = PaymentEvent.builder()
                .paymentId(savedPayment.getId())
                .orderId(savedPayment.getOrderId())
                .customerId(savedPayment.getCustomerId())
                .amount(savedPayment.getAmount())
                .status(savedPayment.getStatus())
                .eventType("PAYMENT_COMPLETED")
                .build();

        kafkaTemplate.send("payment-events", successEvent);
        log.info("Successfully processed payment for order {}", savedPayment.getOrderId());
    }

    private void handlePaymentFailure(OrderEvent orderEvent, Exception ex) {
        log.error("Payment processing failed for order {}", orderEvent.getOrderId(), ex);

        PaymentEvent failureEvent = PaymentEvent.builder()
                .orderId(orderEvent.getOrderId())
                .customerId(orderEvent.getCustomerId())
                .amount(orderEvent.getAmount())
                .status(PaymentStatus.FAILED)
                .eventType("PAYMENT_FAILED")
                .build();

        kafkaTemplate.send("payment-events", failureEvent);
    }

    @Transactional
    public Payment refundPayment(Long orderId) {
        log.info("Processing refund for order {}", orderId);

        Payment payment = paymentRepository.findByOrderId(orderId)
                .orElseThrow(() -> new PaymentNotFoundException(orderId));

        if (payment.getStatus() != PaymentStatus.COMPLETED) {
            throw new PaymentProcessingException("Only completed payments can be refunded");
        }

        try {
            boolean refundSuccess = paymentGateway.processRefund(
                    payment.getCustomerId(),
                    payment.getAmount()
            );

            if (!refundSuccess) {
                throw new PaymentProcessingException("Refund failed in payment gateway");
            }

            payment.setStatus(PaymentStatus.REFUNDED);
            Payment refundedPayment = paymentRepository.save(payment);

            sendRefundEvent(refundedPayment);
            return refundedPayment;
        } catch (Exception ex) {
            log.error("Refund failed for order {}", orderId, ex);
            throw new PaymentProcessingException("Refund processing error", ex);
        }
    }

    private void sendRefundEvent(Payment payment) {
        PaymentEvent refundEvent = PaymentEvent.builder()
                .paymentId(payment.getId())
                .orderId(payment.getOrderId())
                .customerId(payment.getCustomerId())
                .amount(payment.getAmount())
                .status(payment.getStatus())
                .eventType("PAYMENT_REFUNDED")
                .build();

        kafkaTemplate.send("payment-events", refundEvent);
    }

    @Transactional(readOnly = true)
    public Payment getPaymentByOrderId(Long orderId) {
        return paymentRepository.findByOrderId(orderId)
                .orElseThrow(() -> new PaymentNotFoundException(orderId));
    }

    @Transactional(readOnly = true)
    public PaymentDetailsResponse getPayment(Long orderId) {
        Payment payment = paymentRepository.findByOrderId(orderId)
                .orElseThrow(() -> new PaymentNotFoundException(orderId));

        return PaymentDetailsResponse.builder()
                .paymentId(payment.getId())
                .orderId(payment.getOrderId())
                .customerId(payment.getCustomerId())
                .amount(payment.getAmount())
                .status(payment.getStatus())
                .processedAt(payment.getProcessedAt())
                .build();
    }

    @Transactional
    public void processPayment(PaymentProcessRequest request) {
        log.info("Processing payment for order {}", request.getOrderId());

        Payment payment = new Payment();
        payment.setOrderId(request.getOrderId());
        payment.setCustomerId(request.getCustomerId());
        payment.setAmount(request.getAmount());
        payment.setStatus(PaymentStatus.PENDING);
        payment.setProcessedAt(LocalDateTime.now());

        try {
            boolean paymentSuccess = paymentGateway.processPayment(
                    request.getCustomerId(),
                    request.getAmount()
            );

            payment.setStatus(paymentSuccess ?
                    PaymentStatus.COMPLETED :
                    PaymentStatus.FAILED);

            paymentRepository.save(payment);

            if (!paymentSuccess) {
                throw new PaymentProcessingException("Payment gateway declined transaction");
            }

            log.info("Payment processed successfully for order {}", request.getOrderId());
        } catch (Exception e) {
            payment.setStatus(PaymentStatus.FAILED);
            paymentRepository.save(payment);
            log.error("Payment processing failed for order {}", request.getOrderId(), e);
            throw new PaymentProcessingException("Payment processing error", e);
        }
    }

    @Transactional
    public void initiateRefund(RefundRequest request) {
        log.info("Initiating refund for order {}", request.getOrderId());

        Payment payment = paymentRepository.findByOrderId(request.getOrderId())
                .orElseThrow(() -> new PaymentNotFoundException(request.getOrderId()));

        if (payment.getStatus() != PaymentStatus.COMPLETED) {
            throw new PaymentProcessingException(
                    "Only completed payments can be refunded. Current status: " + payment.getStatus()
            );
        }

        try {
            boolean refundSuccess = paymentGateway.processRefund(
                    payment.getCustomerId(),
                    request.getRefundAmount() != null ?
                            request.getRefundAmount() :
                            payment.getAmount()
            );

            payment.setStatus(refundSuccess ?
                    PaymentStatus.REFUNDED :
                    PaymentStatus.REFUND_FAILED);

            paymentRepository.save(payment);

            if (!refundSuccess) {
                throw new PaymentProcessingException("Refund failed in payment gateway");
            }

            log.info("Refund processed successfully for order {}", request.getOrderId());
        } catch (Exception e) {
            payment.setStatus(PaymentStatus.REFUND_FAILED);
            paymentRepository.save(payment);
            log.error("Refund processing failed for order {}", request.getOrderId(), e);
            throw new PaymentProcessingException("Refund processing error", e);
        }
    }

    @Transactional(readOnly = true)
    public PaymentStatusResponse getPaymentStatus(Long paymentId) {
        Payment payment = paymentRepository.findById(paymentId)
                .orElseThrow(() -> new PaymentNotFoundException("Payment not found with ID: " + paymentId));

        return PaymentStatusResponse.builder()
                .status(payment.getStatus())
                .message(getStatusMessage(payment.getStatus()))
                .lastUpdated(payment.getProcessedAt())
                .build();
    }

    private String getStatusMessage(PaymentStatus status) {
        return switch (status) {
            case COMPLETED -> "Payment completed successfully";
            case PENDING -> "Payment is being processed";
            case FAILED -> "Payment processing failed";
            case REFUND_PENDING -> "Payment is being refund";
            case REFUNDED -> "Payment was refunded";
            case REFUND_FAILED -> "Refund processing failed";
        };
    }
}