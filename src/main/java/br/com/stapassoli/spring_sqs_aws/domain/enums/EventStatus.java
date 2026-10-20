package br.com.stapassoli.spring_sqs_aws.domain.enums;

public enum EventStatus {
    PENDING,
    PROCESSED,
    FAILED,
    SENT_TO_DLQ
}
