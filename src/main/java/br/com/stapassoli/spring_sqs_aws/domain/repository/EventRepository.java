package br.com.stapassoli.spring_sqs_aws.domain.repository;

import br.com.stapassoli.spring_sqs_aws.domain.entity.EventEntity;
import br.com.stapassoli.spring_sqs_aws.domain.enums.EventStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface EventRepository extends JpaRepository<EventEntity, UUID> {

    Optional<EventEntity> findByEventId(String eventId);

    boolean existsByEventId(String eventId);

    List<EventEntity> findByStatus(EventStatus status);
}
