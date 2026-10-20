package br.com.stapassoli.spring_sqs_aws.consumer.local;

import br.com.stapassoli.spring_sqs_aws.domain.dto.EventMessageDTO;
import br.com.stapassoli.spring_sqs_aws.exception.NonRetryableException;
import br.com.stapassoli.spring_sqs_aws.producer.dlq.DlqPublisher;
import br.com.stapassoli.spring_sqs_aws.service.EventProcessorService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Simulador de fila e listener local utilizando Spring Events + @Async.
 * Permite reproduzir com fidelidade o comportamento assíncrono de um consumidor de fila
 * (como o SQS) no ambiente de desenvolvimento local, sem necessidade de Docker ou AWS.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SimuladorFilaLocalListener {

    private final EventProcessorService eventProcessorService;
    private final DlqPublisher dlqPublisher;
    private final ObjectMapper objectMapper;

    @Async
    @EventListener
    public void onEventoRecebido(EventoLocalMessage message) {
        log.info("""
                
                📥 [SIMULADOR DE FILA SQS] Mensagem consumida da fila interna:
                - MessageId: {}
                - Tentativa (ReceiveCount): {}
                - Timestamp: {}
                """, message.messageId(), message.receiveCount(), message.publishedAt());

        EventMessageDTO dto = message.eventDTO();

        // Se o DTO ainda não foi desserializado (veio apenas como string bruta)
        if (dto == null && message.rawPayload() != null) {
            try {
                dto = objectMapper.readValue(message.rawPayload(), EventMessageDTO.class);
            } catch (JsonProcessingException ex) {
                log.error("[SIMULADOR FILA] Erro crítico de desserialização (Poison Pill): '{}'", message.rawPayload(), ex);
                dlqPublisher.sendToDlq(
                        message.rawPayload(),
                        "JSON_DESERIALIZATION_ERROR: " + ex.getOriginalMessage(),
                        message.messageId(),
                        Map.of(
                                "x-approximate-receive-count", String.valueOf(message.receiveCount()),
                                "x-exception-class", ex.getClass().getName(),
                                "x-poison-pill", "true"
                        )
                );
                eventProcessorService.markAsFailed(null, message.rawPayload(), "POISON_PILL: " + ex.getMessage(), message.receiveCount());
                return;
            }
        }

        try {
            // Executa a regra de negócio central através da camada de serviço
            eventProcessorService.processEvent(dto, message.rawPayload(), message.receiveCount());
            log.info("✅ [SIMULADOR FILA] Evento [eventId='{}'] processado e persistido no H2 com sucesso!", dto.eventId());

        } catch (NonRetryableException ex) {
            log.error("❌ [SIMULADOR FILA] Erro não recuperável no evento [eventId='{}']. Enviando para DLQ.", dto.eventId(), ex);
            dlqPublisher.sendToDlq(
                    message.rawPayload(),
                    "NON_RETRYABLE_ERROR: " + ex.getMessage(),
                    message.messageId(),
                    Map.of(
                            "x-event-id", dto.eventId(),
                            "x-approximate-receive-count", String.valueOf(message.receiveCount()),
                            "x-exception-class", ex.getClass().getName()
                    )
            );
            eventProcessorService.markAsFailed(dto.eventId(), message.rawPayload(), ex.getMessage(), message.receiveCount());

        } catch (Exception ex) {
            log.warn("⚠️ [SIMULADOR FILA] Erro transitório simulado no evento [eventId='{}', tentativa={}]: {}",
                    dto.eventId(), message.receiveCount(), ex.getMessage());

            // Simula a Redrive Policy do AWS SQS: se receiveCount < 3, retenta; caso contrário, vai para DLQ
            if (message.receiveCount() < 3) {
                log.info("🔄 [SIMULADOR FILA] Simulando Visibility Timeout... Agendando reentrega do evento [tentativa={}]",
                        message.receiveCount() + 1);
                try {
                    Thread.sleep(1000); // Breve espera para simular visibilidade
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
                onEventoRecebido(new EventoLocalMessage(
                        message.messageId(),
                        message.rawPayload(),
                        dto,
                        message.receiveCount() + 1,
                        message.publishedAt()
                ));
            } else {
                log.error("🚨 [SIMULADOR FILA] Limite de tentativas atingido (maxReceiveCount=3). Movendo para DLQ.");
                dlqPublisher.sendToDlq(
                        message.rawPayload(),
                        "MAX_RECEIVE_COUNT_EXCEEDED: " + ex.getMessage(),
                        message.messageId(),
                        Map.of(
                                "x-event-id", dto.eventId(),
                                "x-max-receive-count", "3",
                                "x-exception-class", ex.getClass().getName()
                        )
                );
                eventProcessorService.markAsFailed(dto.eventId(), message.rawPayload(), "DLQ: " + ex.getMessage(), message.receiveCount());
            }
        }
    }
}
