package org.demo.project.dockersample.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DTO for Event Response data retrieved from the API
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RetrieveEventResponse {
    private String eventId;
    private String accountNumber;
    private String eventData;
    private String status;
}

