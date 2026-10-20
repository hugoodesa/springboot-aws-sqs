package br.com.stapassoli.spring_sqs_aws.service;

import br.com.stapassoli.spring_sqs_aws.domain.dto.EventMessageDTO;
import br.com.stapassoli.spring_sqs_aws.domain.entity.EventEntity;
import br.com.stapassoli.spring_sqs_aws.domain.enums.EventStatus;
import br.com.stapassoli.spring_sqs_aws.domain.repository.EventRepository;
import br.com.stapassoli.spring_sqs_aws.exception.BusinessException;
import br.com.stapassoli.spring_sqs_aws.exception.NonRetryableException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EventProcessorServiceTest {

    @Mock
    private EventRepository eventRepository;

    @InjectMocks
    private EventProcessorService eventProcessorService;

    private EventMessageDTO sampleDTO;
    private String rawJson;

    @BeforeEach
    void setUp() {
        sampleDTO = new EventMessageDTO(
                "evt-12345",
                "ORDER_CREATED",
                "ORDER_SERVICE",
                Instant.now(),
                Map.of("orderId", "ord-999", "amount", 150.0)
        );
        rawJson = "{\"eventId\":\"evt-12345\",\"eventType\":\"ORDER_CREATED\"}";
    }

    @Test
    @DisplayName("Deve processar e persistir um novo evento com status PROCESSED")
    void shouldProcessAndPersistNewEventSuccessfully() {
        when(eventRepository.findByEventId("evt-12345")).thenReturn(Optional.empty());
        when(eventRepository.save(any(EventEntity.class))).thenAnswer(invocation -> {
            EventEntity e = invocation.getArgument(0);
            e.setId(UUID.randomUUID());
            return e;
        });

        EventEntity result = eventProcessorService.processEvent(sampleDTO, rawJson, 1);

        assertThat(result).isNotNull();
        assertThat(result.getEventId()).isEqualTo("evt-12345");
        assertThat(result.getStatus()).isEqualTo(EventStatus.PROCESSED);
        assertThat(result.getProcessedAt()).isNotNull();

        ArgumentCaptor<EventEntity> captor = ArgumentCaptor.forClass(EventEntity.class);
        verify(eventRepository).save(captor.capture());
        EventEntity saved = captor.getValue();
        assertThat(saved.getStatus()).isEqualTo(EventStatus.PROCESSED);
        assertThat(saved.getRetryCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("Deve garantir idempotência quando evento já foi processado com sucesso")
    void shouldBeIdempotentWhenEventAlreadyProcessed() {
        EventEntity existing = EventEntity.builder()
                .id(UUID.randomUUID())
                .eventId("evt-12345")
                .eventType("ORDER_CREATED")
                .status(EventStatus.PROCESSED)
                .processedAt(Instant.now().minusSeconds(60))
                .build();

        when(eventRepository.findByEventId("evt-12345")).thenReturn(Optional.of(existing));

        EventEntity result = eventProcessorService.processEvent(sampleDTO, rawJson, 2);

        assertThat(result).isEqualTo(existing);
        verify(eventRepository, never()).save(any());
    }

    @Test
    @DisplayName("Deve lançar NonRetryableException quando eventId for nulo")
    void shouldThrowNonRetryableExceptionWhenEventIdIsNull() {
        EventMessageDTO invalidDTO = new EventMessageDTO(null, "TYPE", "SRC", Instant.now(), Map.of());

        assertThatThrownBy(() -> eventProcessorService.processEvent(invalidDTO, "{}", 1))
                .isInstanceOf(NonRetryableException.class)
                .hasMessageContaining("eventId");

        verify(eventRepository, never()).save(any());
    }

    @Test
    @DisplayName("Deve lançar NonRetryableException quando regra fatal for acionada")
    void shouldThrowNonRetryableExceptionOnFatalBusinessRule() {
        EventMessageDTO fatalDTO = new EventMessageDTO("evt-fatal", "SIMULATE_NON_RETRYABLE_ERROR", "SRC", Instant.now(), Map.of());
        when(eventRepository.findByEventId("evt-fatal")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> eventProcessorService.processEvent(fatalDTO, "{}", 1))
                .isInstanceOf(NonRetryableException.class);
    }

    @Test
    @DisplayName("Deve lançar BusinessException quando falha recuperável ocorrer")
    void shouldThrowBusinessExceptionOnTransientError() {
        EventMessageDTO retryDTO = new EventMessageDTO("evt-retry", "SIMULATE_RETRYABLE_ERROR", "SRC", Instant.now(), Map.of());
        when(eventRepository.findByEventId("evt-retry")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> eventProcessorService.processEvent(retryDTO, "{}", 1))
                .isInstanceOf(BusinessException.class);
    }
}
