package br.com.stapassoli.spring_sqs_aws.producer.dlq;

import java.util.Map;

/**
 * Interface desacoplada para publicação em Dead Letter Queue (DLQ).
 * Permite alternar de forma transparente entre o envio real via AWS SQS e
 * o envio simulado em memória para desenvolvimento local sem infraestrutura.
 */
public interface DlqPublisher {

    void sendToDlq(String originalPayload, String reason, String originalMessageId, Map<String, Object> additionalHeaders);
}
