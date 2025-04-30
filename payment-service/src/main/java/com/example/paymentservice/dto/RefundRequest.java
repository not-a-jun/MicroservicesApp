package com.example.paymentservice.dto;

import jakarta.validation.constraints.*;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@Builder
public class RefundRequest {
    @NotNull
    private Long orderId;

    @NotBlank
    private String reason;

    @PositiveOrZero
    private BigDecimal refundAmount;
}