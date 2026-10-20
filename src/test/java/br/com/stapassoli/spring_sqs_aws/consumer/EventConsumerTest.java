package br.com.stapassoli.spring_sqs_aws.consumer;

import br.com.stapassoli.spring_sqs_aws.domain.dto.EventMessageDTO;
import br.com.stapassoli.spring_sqs_aws.exception.BusinessException;
import br.com.stapassoli.spring_sqs_aws.exception.NonRetryableException;
import br.com.stapassoli.spring_sqs_aws.producer.dlq.DlqPublisher;
import br.com.stapassoli.spring_sqs_aws.service.EventProcessorService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EventConsumerTest {

    @Mock
    private EventProcessorService eventProcessorService;

    @Mock
    private DlqPublisher dlqPublisher;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());

    @InjectMocks
    private EventConsumer eventConsumer;

    private String validPayload;

    @BeforeEach
    void setUp() {
        validPayload = """
                {
                    "eventId": "evt-abc-123",
                    "eventType": "PAYMENT_CONFIRMED",
                    "source": "PAYMENT_GATEWAY",
                    "timestamp": "2026-10-17T12:00:00Z",
                    "data": {
                        "amount": 250.75
                    }
                }
                """;
    }

    @Test
    @DisplayName("Deve processar com sucesso quando payload JSON for válido")
    void shouldProcessSuccessfullyWhenPayloadIsValid() {
        eventConsumer.consume(validPayload, "1", "msg-id-1", "receipt-1");

        verify(eventProcessorService).processEvent(any(EventMessageDTO.class), eq(validPayload), eq(1));
        verify(dlqPublisher, never()).sendToDlq(anyString(), anyString(), anyString(), anyMap());
    }

    @Test
    @DisplayName("Deve enviar diretamente para a DLQ e não relançar exceção quando payload for JSON inválido (Poison Pill)")
    void shouldSendToDlqOnDeserializationErrorWithoutThrowing() {
        String invalidJson = "{ invalid-json: not_quoted ";

        eventConsumer.consume(invalidJson, "1", "msg-id-corrupt", "receipt-2");

        verify(dlqPublisher).sendToDlq(
                eq(invalidJson),
                contains("JSON_DESERIALIZATION_ERROR"),
                eq("msg-id-corrupt"),
                anyMap()
        );
        verify(eventProcessorService).markAsFailed(eq(null), eq(invalidJson), contains("POISON_PILL"), eq(1));
        verify(eventProcessorService, never()).processEvent(any(), any(), anyInt());
    }

    @Test
    @DisplayName("Deve enviar para DLQ quando ocorrer NonRetryableException")
    void shouldSendToDlqOnNonRetryableException() {
        when(eventProcessorService.processEvent(any(), any(), anyInt()))
                .thenThrow(new NonRetryableException("Violacao estrutural de contrato"));

        eventConsumer.consume(validPayload, "1", "msg-id-3", "receipt-3");

        verify(dlqPublisher).sendToDlq(
                eq(validPayload),
                contains("NON_RETRYABLE_ERROR"),
                eq("msg-id-3"),
                anyMap()
        );
        verify(eventProcessorService).markAsFailed(eq("evt-abc-123"), eq(validPayload), contains("Violacao"), eq(1));
    }

    @Test
    @DisplayName("Deve relançar exceção quando for erro recuperável para acionar a Redrive Policy / Visibility Timeout")
    void shouldRethrowExceptionOnTransientErrorToTriggerSqsRedrive() {
        when(eventProcessorService.processEvent(any(), any(), anyInt()))
                .thenThrow(new BusinessException("Database deadlock temporario"));

        assertThatThrownBy(() -> eventConsumer.consume(validPayload, "1", "msg-id-4", "receipt-4"))
                .isInstanceOf(BusinessException.class)
                .hasMessageContaining("deadlock");

        verify(dlqPublisher, never()).sendToDlq(anyString(), anyString(), anyString(), anyMap());
    }
}
