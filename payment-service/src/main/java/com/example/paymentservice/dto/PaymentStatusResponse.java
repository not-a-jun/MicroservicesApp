package com.example.paymentservice.dto;

import com.example.common.model.PaymentStatus;
import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Builder
public class PaymentStatusResponse {
    private PaymentStatus status;
    private String message;
    private LocalDateTime lastUpdated;
}