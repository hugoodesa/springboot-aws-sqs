package br.com.stapassoli.spring_sqs_aws.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI openAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("cor-invoice-bank-return-wrk")
                        .version("0.0.1-SNAPSHOT")
                        .description("""
                                Worker de retorno bancário de notas fiscais.
                                Consome eventos do Hub via SQS e também expõe endpoints HTTP
                                para testes locais, consulta paginada e simulação de fila.
                                Documentação interativa: /swagger-ui.html
                                """));
    }
}
