package br.com.stapassoli.spring_sqs_aws.producer.queue;

import br.com.stapassoli.spring_sqs_aws.consumer.local.EventoLocalMessage;
import br.com.stapassoli.spring_sqs_aws.domain.dto.EventMessageDTO;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationEventPublisher;

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class LocalQueueProducerTest {

    @Mock
    private ApplicationEventPublisher eventPublisher;

    private ObjectMapper objectMapper;
    private LocalQueueProducer producer;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        producer = new LocalQueueProducer(eventPublisher, objectMapper);
    }

    @Test
    @DisplayName("Deve publicar EventoLocalMessage via ApplicationEventPublisher")
    void shouldPublishLocalMessage() {
        String payload = "{\"eventId\":\"evt-local-1\",\"eventType\":\"TEST\"}";
        producer.send(payload);

        ArgumentCaptor<EventoLocalMessage> captor = ArgumentCaptor.forClass(EventoLocalMessage.class);
        verify(eventPublisher).publishEvent(captor.capture());

        EventoLocalMessage message = captor.getValue();
        assertThat(message.rawPayload()).isEqualTo(payload);
        assertThat(message.messageId()).isNotBlank();
        assertThat(message.receiveCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("Deve serializar EventMessageDTO e publicar no ApplicationEventPublisher")
    void shouldSerializeDTOAndPublish() {
        var dto = new EventMessageDTO("evt-local-2", "ITEM_ADDED", "TEST", null, Map.of());
        producer.send(dto);

        ArgumentCaptor<EventoLocalMessage> captor = ArgumentCaptor.forClass(EventoLocalMessage.class);
        verify(eventPublisher).publishEvent(captor.capture());

        EventoLocalMessage message = captor.getValue();
        assertThat(message.rawPayload()).contains("evt-local-2");
        assertThat(message.eventDTO()).isNotNull();
        assertThat(message.eventDTO().eventId()).isEqualTo("evt-local-2");
    }
}
