package br.com.stapassoli.spring_sqs_aws.domain.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.Map;

/**
 * DTO que representa o payload do evento consumido do hub de eventos AWS SQS.
 * Utiliza o recurso de Records do Java 21 para imutabilidade e concisão.
 *
 * O projeto recebe mensagens do EventBridge, cujo envelope principal usa os campos
 * "detail" e "detail-type". A assinatura permanece compatível com os testes e
 * integrações existentes, aceitando também o formato do EventBridge.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(name = "EventMessage", description = "Envelope do evento. No EventBridge, os dados de negócio ficam em detail.")
public record EventMessageDTO(
        @JsonAlias({"id", "eventId"})
        @JsonProperty("id")
        @Schema(description = "Identificador do evento (id no EventBridge).", example = "IdHubdeEventos")
        String eventId,

        @JsonAlias({"detail-type", "detailType", "eventType"})
        @JsonProperty("detail-type")
        @Schema(description = "Tipo do evento (detail-type no EventBridge).", example = "finance.invoice.bank-return")
        String eventType,

        @JsonProperty("source")
        @Schema(description = "Origem do evento.", example = "event-hub")
        String source,

        @JsonAlias({"time", "timestamp"})
        @JsonProperty("time")
        @Schema(description = "Instante do evento (time no EventBridge).", example = "2026-05-08T12:57:08Z")
        Instant timestamp,

        @JsonAlias({"datacontenttype", "dataContentType", "data-content-type"})
        @JsonProperty("datacontenttype")
        @Schema(description = "Tipo de conteúdo do payload (datacontenttype no EventBridge).", example = "application/json")
        String datacontenttype,

        @JsonProperty("data")
        @Schema(description = "Payload legado. Preferir detail para mensagens do EventBridge.")
        Map<String, Object> data,

        @JsonProperty("detail")
        @Schema(description = "Dados de negócio do EventBridge (invoiceId, chave composta e payment).")
        Map<String, Object> detail
) {
    public EventMessageDTO {
        if (timestamp == null) {
            timestamp = Instant.now();
        }
    }

    public EventMessageDTO(String eventId, String eventType, String source, Instant timestamp, Map<String, Object> data) {
        this(eventId, eventType, source, timestamp, null, data, null);
    }

    public EventMessageDTO(String eventId, String eventType, String source, Instant timestamp, Map<String, Object> data, Map<String, Object> detail) {
        this(eventId, eventType, source, timestamp, null, data, detail);
    }

    public static EventMessageDTO fromEventBridgeExample() {
        return new EventMessageDTO(
                "IdHubdeEventos",
                "Tipo de vento finance.invoice.",
                "Origem do evento",
                Instant.parse("2026-05-08T12:57:08Z"),
                null,
                Map.of(
                        "invoiceId", "CHAVEPORTALNACIONAL",
                        "payment", java.util.List.of(Map.of("key", "Prefixo+idseqPG")),
                        "invoiceStatus", "Pago"
                )
        );
    }

    /**
     * Indica se o evento possui o nó de negócio 'detail' informado.
     */
    public boolean hasBusinessDetail() {
        return (detail != null) || (data != null && data.containsKey("detail"));
    }

    /**
     * Extrai o mapa de negócio a partir de 'detail' (EventBridge) ou data.detail.
     * Retorna null caso o nó 'detail' não esteja presente.
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> getBusinessDetail() {
        if (detail != null) {
            return detail;
        }

        if (data != null && data.containsKey("detail") && data.get("detail") instanceof Map<?, ?> nestedDetail) {
            return (Map<String, Object>) nestedDetail;
        }

        return null;
    }
}
