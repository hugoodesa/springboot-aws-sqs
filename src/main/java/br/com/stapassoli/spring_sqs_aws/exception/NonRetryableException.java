package br.com.stapassoli.spring_sqs_aws.exception;

/**
 * Exceção para erros não recuperáveis (Poison Pills, schema inválido, violações de regras definitivas).
 * Deve causar o envio imediato para a Dead Letter Queue (DLQ) sem queimar ciclos desnecessários de retentativas.
 */
public class NonRetryableException extends RuntimeException {
    public NonRetryableException(String message) {
        super(message);
    }

    public NonRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
