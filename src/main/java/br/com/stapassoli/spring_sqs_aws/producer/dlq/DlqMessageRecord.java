package br.com.stapassoli.spring_sqs_aws.producer.dlq;

import java.time.Instant;
import java.util.Map;

public record DlqMessageRecord(
        String payload,
        String reason,
        String originalMessageId,
        Instant failedAt,
        Map<String, Object> headers
) {}
