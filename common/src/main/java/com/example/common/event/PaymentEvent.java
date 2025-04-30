package com.example.common.event;

import com.example.common.model.PaymentStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class PaymentEvent {
    private Long paymentId;
    private String eventType;
    private Long orderId;
    private String customerId;
    private BigDecimal amount;
    private PaymentStatus status;
    private String sagaId;
    private int retryCount;
    private String failureReason;
}