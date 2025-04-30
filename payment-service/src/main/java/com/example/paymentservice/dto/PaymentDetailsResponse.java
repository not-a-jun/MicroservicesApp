package com.example.paymentservice.dto;

import com.example.common.model.PaymentStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentDetailsResponse {
    private Long paymentId;
    private Long orderId;
    private String customerId;
    private BigDecimal amount;
    private PaymentStatus status;
    private LocalDateTime processedAt;
}
