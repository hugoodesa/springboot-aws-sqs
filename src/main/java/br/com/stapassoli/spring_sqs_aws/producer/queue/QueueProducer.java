package br.com.stapassoli.spring_sqs_aws.producer.queue;

import br.com.stapassoli.spring_sqs_aws.domain.dto.EventMessageDTO;

import java.util.Map;

/**
 * Interface para publicação em filas normais (não-DLQ).
 * Permite envio de mensagens tanto em formato de modelo de domínio/DTO quanto JSON bruto (String),
 * com suporte a cabeçalhos customizados e abstração completa de infraestrutura.
 */
public interface QueueProducer {

    /**
     * Envia um objeto de evento para a fila principal configurada.
     */
    void send(EventMessageDTO event);

    /**
     * Envia um objeto de evento com cabeçalhos adicionais para a fila principal configurada.
     */
    void send(EventMessageDTO event, Map<String, Object> headers);

    /**
     * Envia uma carga útil bruta (JSON String) para a fila principal configurada.
     */
    void send(String rawPayload);

    /**
     * Envia uma carga útil bruta com cabeçalhos adicionais para a fila principal configurada.
     */
    void send(String rawPayload, Map<String, Object> headers);

    /**
     * Envia uma carga útil bruta com cabeçalhos para uma fila específica pelo nome.
     */
    void sendToQueue(String queueName, String rawPayload, Map<String, Object> headers);
}
