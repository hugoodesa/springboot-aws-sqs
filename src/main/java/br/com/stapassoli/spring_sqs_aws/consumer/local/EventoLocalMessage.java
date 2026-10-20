package br.com.stapassoli.spring_sqs_aws.consumer.local;

import br.com.stapassoli.spring_sqs_aws.domain.dto.EventMessageDTO;

import java.time.Instant;
import java.util.UUID;

/**
 * Mensagem de evento interno utilizada para simular o recebimento de mensagens do SQS
 * através do ApplicationEventPublisher do Spring em ambiente de desenvolvimento local.
 */
public record EventoLocalMessage(
        String messageId,
        String rawPayload,
        EventMessageDTO eventDTO,
        int receiveCount,
        Instant publishedAt
) {
    public EventoLocalMessage(String rawPayload, EventMessageDTO eventDTO, int receiveCount) {
        this(UUID.randomUUID().toString(), rawPayload, eventDTO, receiveCount, Instant.now());
    }
}
