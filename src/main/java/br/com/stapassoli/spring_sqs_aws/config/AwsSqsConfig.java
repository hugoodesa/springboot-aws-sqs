package br.com.stapassoli.spring_sqs_aws.config;

import io.awspring.cloud.sqs.config.SqsMessageListenerContainerFactory;
import io.awspring.cloud.sqs.listener.acknowledgement.handler.AcknowledgementMode;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.net.URI;
import java.time.Duration;

@Slf4j
@Configuration
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.sqs.enabled", havingValue = "true")
public class AwsSqsConfig {

    private final AwsSqsProperties properties;

    @Value("${spring.cloud.aws.region.static:us-east-1}")
    private String awsRegion;

    @Value("${spring.cloud.aws.credentials.access-key:test}")
    private String accessKey;

    @Value("${spring.cloud.aws.credentials.secret-key:test}")
    private String secretKey;

    @Value("${spring.cloud.aws.sqs.endpoint:#{null}}")
    private String customEndpoint;

    /**
     * Cliente assíncrono do AWS SDK v2 para o SQS.
     * Suporta tanto endpoint customizado (LocalStack/Testcontainers) quanto endpoint nativo da AWS.
     */
    @Bean
    public SqsAsyncClient sqsAsyncClient() {
        log.info("Inicializando SqsAsyncClient para região '{}' com endpoint customizado: '{}'", awsRegion, customEndpoint);

        var builder = SqsAsyncClient.builder()
                .region(Region.of(awsRegion))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)
                ));

        if (customEndpoint != null && !customEndpoint.isBlank()) {
            builder.endpointOverride(URI.create(customEndpoint));
        }

        return builder.build();
    }

    /**
     * Template de alto nível do Spring Cloud AWS 3.x para envio e manipulação de mensagens no SQS.
     */
    @Bean
    public SqsTemplate sqsTemplate(SqsAsyncClient sqsAsyncClient) {
        return SqsTemplate.builder()
                .sqsAsyncClient(sqsAsyncClient)
                .build();
    }

    /**
     * Fábrica de containers de escuta do @SqsListener.
     * Define AcknowledgementMode.ON_SUCCESS para garantir que mensagens com falha não sejam reconhecidas
     * e retornem à fila (respeitando o Visibility Timeout e Redrive Policy para DLQ da AWS).
     */
    @Bean
    public SqsMessageListenerContainerFactory<Object> defaultSqsListenerContainerFactory(
            SqsAsyncClient sqsAsyncClient,
            CustomSqsErrorHandler customSqsErrorHandler) {
        return SqsMessageListenerContainerFactory
                .builder()
                .sqsAsyncClient(sqsAsyncClient)
                .errorHandler(customSqsErrorHandler)
                .configure(options -> options
                        .acknowledgementMode(AcknowledgementMode.ON_SUCCESS)
                        .maxConcurrentMessages(properties.getListener().getMaxConcurrentMessages())
                        .maxMessagesPerPoll(properties.getListener().getMaxMessagesPerPoll())
                        .pollTimeout(Duration.ofSeconds(properties.getListener().getPollTimeoutSeconds()))
                )
                .build();
    }
}
