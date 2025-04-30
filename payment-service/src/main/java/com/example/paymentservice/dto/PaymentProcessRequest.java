package com.example.paymentservice.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@Builder
public class PaymentProcessRequest {
    @NotNull
    private Long orderId;

    @NotBlank
    private String customerId;

    @Positive
    private BigDecimal amount;

    @Pattern(regexp = "[A-Z]{3}")
    private String currency;
}
