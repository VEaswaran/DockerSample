package org.demo.project.dockersample.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class EventData {
    private String serviceType;
    private String eventId;
    private String accountNumber;
    private String messageContent;
    private Integer retryCount;
    private Long offsetPosition;
    private Integer partitionNumber;
}

