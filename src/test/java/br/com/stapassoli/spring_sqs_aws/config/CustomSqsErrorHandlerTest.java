package br.com.stapassoli.spring_sqs_aws.config;

import br.com.stapassoli.spring_sqs_aws.exception.BusinessException;
import br.com.stapassoli.spring_sqs_aws.exception.NonRetryableException;
import br.com.stapassoli.spring_sqs_aws.producer.dlq.DlqPublisher;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.support.MessageBuilder;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class CustomSqsErrorHandlerTest {

    @Mock
    private DlqPublisher dlqPublisher;

    @InjectMocks
    private CustomSqsErrorHandler errorHandler;

    @Test
    @DisplayName("Deve enviar diretamente para a DLQ quando exceção for NonRetryableException")
    void shouldSendToDlqOnNonRetryableException() {
        Message<Object> message = MessageBuilder.<Object>withPayload("{\"test\":\"data\"}").build();
        String messageId = message.getHeaders().getId().toString();

        errorHandler.handle(message, new NonRetryableException("Erro fatal"));

        verify(dlqPublisher).sendToDlq(
                eq("{\"test\":\"data\"}"),
                eq("Erro fatal"),
                eq(messageId),
                anyMap()
        );
    }

    @Test
    @DisplayName("Deve enviar diretamente para a DLQ quando exceção for MessageConversionException")
    void shouldSendToDlqOnMessageConversionException() {
        Message<Object> message = MessageBuilder.<Object>withPayload("bad-payload").build();
        String messageId = message.getHeaders().getId().toString();

        errorHandler.handle(message, new MessageConversionException("Falha na conversao"));

        verify(dlqPublisher).sendToDlq(
                eq("bad-payload"),
                contains("Falha na conversao"),
                eq(messageId),
                anyMap()
        );
    }

    @Test
    @DisplayName("Deve relançar exceção quando falha for transitória para acionar SQS Redrive Policy")
    void shouldRethrowOnTransientFailure() {
        Message<Object> message = MessageBuilder.<Object>withPayload("{\"test\":\"data\"}").build();

        assertThatThrownBy(() -> errorHandler.handle(message, new BusinessException("Falha temporária no banco")))
                .isInstanceOf(BusinessException.class)
                .hasMessageContaining("Falha temporária no banco");

        verify(dlqPublisher, never()).sendToDlq(any(), any(), any(), any());
    }
}
