package br.com.stapassoli.spring_sqs_aws.producer.dlq;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Implementação de DLQ em memória para ambiente de desenvolvimento local (sem AWS/Docker).
 * Permite registrar e auditar mensagens que seriam descartadas/enviadas para a DLQ do SQS.
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "app.sqs.enabled", havingValue = "false", matchIfMissing = true)
public class LocalMockDlqPublisher implements DlqPublisher {

    private final List<DlqMessageRecord> capturedMessages = new CopyOnWriteArrayList<>();

    @Override
    public void sendToDlq(String originalPayload, String reason, String originalMessageId, Map<String, Object> additionalHeaders) {
        var record = new DlqMessageRecord(
                originalPayload,
                reason,
                originalMessageId,
                Instant.now(),
                additionalHeaders != null ? additionalHeaders : Map.of()
        );

        capturedMessages.add(record);

        log.warn("""
                
                ========================= [LOCAL DLQ SIMULATOR] =========================
                ⚠️  Mensagem direcionada para a Dead Letter Queue (Modo Local):
                - Motivo do Erro: {}
                - MessageId Original: {}
                - Timestamp: {}
                - Headers Adicionais: {}
                - Payload Conteúdo:
                {}
                =========================================================================
                """,
                reason, originalMessageId, record.failedAt(), record.headers(), originalPayload);
    }

    public List<DlqMessageRecord> getCapturedMessages() {
        return Collections.unmodifiableList(capturedMessages);
    }

    public void clear() {
        capturedMessages.clear();
    }
}
