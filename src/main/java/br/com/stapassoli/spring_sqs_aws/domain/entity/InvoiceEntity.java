package br.com.stapassoli.spring_sqs_aws.domain.entity;

import br.com.stapassoli.spring_sqs_aws.domain.enums.InvoiceStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.time.Instant;
import java.util.UUID;

/**
 * Entidade que representa a Nota Fiscal e seu status de pagamento.
 */
@Entity
@Table(
        name = "tb_invoices",
        indexes = {
                @Index(name = "idx_invoice_id", columnList = "invoice_id"),
                @Index(name = "idx_invoice_composite", columnList = "document, issue_date, type, number, serie")
        }
)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class InvoiceEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "invoice_id", length = 100)
    private String invoiceId;

    @Column(name = "document", length = 30)
    private String document;

    @Column(name = "issue_date", length = 50)
    private String issueDate;

    @Column(name = "type", length = 30)
    private String type;

    @Column(name = "number", length = 50)
    private String number;

    @Column(name = "serie", length = 30)
    private String serie;

    @Column(name = "status", nullable = false, length = 30)
    private String status;

    @Column(name = "last_payment_key", length = 100)
    private String lastPaymentKey;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at")
    private Instant updatedAt;

    @PrePersist
    public void prePersist() {
        if (this.createdAt == null) {
            this.createdAt = Instant.now();
        }
        if (this.status == null) {
            this.status = InvoiceStatus.PENDENTE.getDescription();
        }
    }

    @PreUpdate
    public void preUpdate() {
        this.updatedAt = Instant.now();
    }
}
