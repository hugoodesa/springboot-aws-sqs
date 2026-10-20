package br.com.stapassoli.spring_sqs_aws;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAsync
@SpringBootApplication
public class SpringSqsAwsApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringSqsAwsApplication.class, args);
	}

}
