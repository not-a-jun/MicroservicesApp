package com.example.ticketservice.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.NOT_FOUND)
public class TourNotFoundException extends RuntimeException {

    public  TourNotFoundException(String attractionId) {
        super(String.format("Tour not found with attraction ID: %s", attractionId));
    }

    public TourNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
