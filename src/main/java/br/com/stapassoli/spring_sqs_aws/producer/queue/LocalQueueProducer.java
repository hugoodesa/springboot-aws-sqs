package br.com.stapassoli.spring_sqs_aws.producer.queue;

import br.com.stapassoli.spring_sqs_aws.consumer.local.EventoLocalMessage;
import br.com.stapassoli.spring_sqs_aws.domain.dto.EventMessageDTO;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Implementação local para envio em fila simulada em memória.
 * Utiliza o ApplicationEventPublisher do Spring para disparar o SimuladorFilaLocalListener.
 * Ativada quando app.sqs.enabled=false (ambiente sem AWS/Docker).
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.sqs.enabled", havingValue = "false", matchIfMissing = true)
public class LocalQueueProducer implements QueueProducer {

    private final ApplicationEventPublisher eventPublisher;
    private final ObjectMapper objectMapper;

    @Override
    public void send(EventMessageDTO event) {
        send(event, Map.of());
    }

    @Override
    public void send(EventMessageDTO event, Map<String, Object> headers) {
        try {
            String payload = objectMapper.writeValueAsString(event);
            sendToQueue("local-simulated-queue", payload, headers);
        } catch (JsonProcessingException e) {
            log.error("[LOCAL Queue Producer] Erro ao serializar evento: {}", event, e);
            throw new IllegalArgumentException("Falha na serialização do evento", e);
        }
    }

    @Override
    public void send(String rawPayload) {
        sendToQueue("local-simulated-queue", rawPayload, Map.of());
    }

    @Override
    public void send(String rawPayload, Map<String, Object> headers) {
        sendToQueue("local-simulated-queue", rawPayload, headers);
    }

    @Override
    public void sendToQueue(String queueName, String rawPayload, Map<String, Object> headers) {
        log.info("[LOCAL Queue Producer] Postando mensagem na fila local '{}': {}", queueName, rawPayload);

        EventMessageDTO dto = null;
        try {
            dto = objectMapper.readValue(rawPayload, EventMessageDTO.class);
        } catch (Exception ignored) {
            // Se for JSON inválido, o listener detectará o Poison Pill
        }

        var message = new EventoLocalMessage(rawPayload, dto, 1);
        eventPublisher.publishEvent(message);
        log.info("[LOCAL Queue Producer] Evento publicado no barramento local com MessageId '{}'", message.messageId());
    }
}
