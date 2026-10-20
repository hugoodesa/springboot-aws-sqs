package br.com.stapassoli.spring_sqs_aws.producer.queue;

import br.com.stapassoli.spring_sqs_aws.config.AwsSqsProperties;
import br.com.stapassoli.spring_sqs_aws.domain.dto.EventMessageDTO;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.messaging.Message;

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SqsQueueProducerTest {

    @Mock
    private SqsTemplate sqsTemplate;

    @Mock
    private AwsSqsProperties properties;

    @Mock
    private AwsSqsProperties.Queues queues;

    private ObjectMapper objectMapper;
    private SqsQueueProducer producer;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        producer = new SqsQueueProducer(sqsTemplate, properties, objectMapper);
    }

    @Test
    @DisplayName("Deve enviar mensagem String para fila SQS com cabeçalhos corretos")
    void shouldSendRawPayloadToSqsQueue() {
        when(properties.getQueues()).thenReturn(queues);
        when(queues.getEventsQueue()).thenReturn("events-hub-queue");

        String payload = "{\"teste\":\"valor\"}";
        producer.send(payload, Map.of("x-custom-header", "123"));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Message<String>> messageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(sqsTemplate).send(eq("events-hub-queue"), messageCaptor.capture());

        Message<String> sentMessage = messageCaptor.getValue();
        assertThat(sentMessage.getPayload()).isEqualTo(payload);
        assertThat(sentMessage.getHeaders().get("x-custom-header")).isEqualTo("123");
        assertThat(sentMessage.getHeaders().get("x-produced-at")).isNotNull();
        assertThat(sentMessage.getHeaders().get("x-message-id")).isNotNull();
    }

    @Test
    @DisplayName("Deve serializar EventMessageDTO e enviar para a fila SQS")
    void shouldSerializeDTOAndSendToSqs() {
        when(properties.getQueues()).thenReturn(queues);
        when(queues.getEventsQueue()).thenReturn("events-hub-queue");

        var dto = new EventMessageDTO("evt-99", "ORDER_CREATED", "APP", null, Map.of("total", 150));
        producer.send(dto);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Message<String>> messageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(sqsTemplate).send(eq("events-hub-queue"), messageCaptor.capture());

        Message<String> sentMessage = messageCaptor.getValue();
        assertThat(sentMessage.getPayload()).contains("evt-99");
        assertThat(sentMessage.getPayload()).contains("ORDER_CREATED");
    }
}
