package com.example.common.model;

public enum TicketStatus {
    PENDING,      // Бронь создана, ожидает подтверждения доступности
    AVAILABLE,    // Место доступно (подтверждено availability-service)
    CONFIRMED,    // Билет подтверждён (после успешной оплаты)
    FAILED,       // Окончательная ошибка (не поправимая автоматически)
    CANCELLED     // Бронь отменена (вручную или по таймауту)
}