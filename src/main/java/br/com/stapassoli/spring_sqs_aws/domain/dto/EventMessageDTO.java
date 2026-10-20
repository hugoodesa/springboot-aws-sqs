package br.com.stapassoli.spring_sqs_aws.domain.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Map;

/**
 * DTO que representa o payload do evento consumido do hub de eventos AWS SQS.
 * Utiliza o recurso de Records do Java 21 para imutabilidade e concisão.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record EventMessageDTO(
        @JsonProperty("eventId")
        String eventId,

        @JsonProperty("eventType")
        String eventType,

        @JsonProperty("source")
        String source,

        @JsonProperty("timestamp")
        Instant timestamp,

        @JsonProperty("data")
        Map<String, Object> data
) {
    public EventMessageDTO {
        if (timestamp == null) {
            timestamp = Instant.now();
        }
    }
}
