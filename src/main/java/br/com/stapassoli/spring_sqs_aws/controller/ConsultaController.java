package br.com.stapassoli.spring_sqs_aws.controller;

import br.com.stapassoli.spring_sqs_aws.domain.entity.EventEntity;
import br.com.stapassoli.spring_sqs_aws.domain.entity.InvoiceEntity;
import br.com.stapassoli.spring_sqs_aws.domain.repository.EventRepository;
import br.com.stapassoli.spring_sqs_aws.domain.repository.InvoiceRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/consultas")
@RequiredArgsConstructor
@Tag(name = "Consultas", description = "Listagem paginada dos registros persistidos. Sem page/size, retorna todos os registros.")
public class ConsultaController {

    private final EventRepository eventRepository;
    private final InvoiceRepository invoiceRepository;

    @GetMapping("/eventos")
    @Operation(
            summary = "Listar eventos",
            description = "Retorna os eventos persistidos em formato de página Spring Data. "
                    + "Quando page e size não são informados, a consulta é unpaged e traz todos os registros."
    )
    @ApiResponse(responseCode = "200", description = "Página de eventos",
            content = @Content(schema = @Schema(implementation = EventEntity.class)))
    public ResponseEntity<Page<EventEntity>> listarEventos(
            @Parameter(description = "Número da página (base 0). Omitir juntamente com size para trazer todos os registros.")
            @RequestParam(required = false) Integer page,
            @Parameter(description = "Quantidade de registros por página. Omitir para trazer todos os registros.")
            @RequestParam(required = false) Integer size,
            @Parameter(description = "Campo de ordenação da entidade EventEntity.")
            @RequestParam(defaultValue = "createdAt") String sort,
            @Parameter(description = "Direção da ordenação.")
            @RequestParam(defaultValue = "DESC") Sort.Direction direction
    ) {
        return ResponseEntity.ok(eventRepository.findAll(toPageable(page, size, sort, direction)));
    }

    @GetMapping("/notas-fiscais")
    @Operation(
            summary = "Listar notas fiscais",
            description = "Retorna as notas fiscais persistidas em formato de página Spring Data. "
                    + "Quando page e size não são informados, a consulta é unpaged e traz todos os registros."
    )
    @ApiResponse(responseCode = "200", description = "Página de notas fiscais",
            content = @Content(schema = @Schema(implementation = InvoiceEntity.class)))
    public ResponseEntity<Page<InvoiceEntity>> listarNotasFiscais(
            @Parameter(description = "Número da página (base 0). Omitir juntamente com size para trazer todos os registros.")
            @RequestParam(required = false) Integer page,
            @Parameter(description = "Quantidade de registros por página. Omitir para trazer todos os registros.")
            @RequestParam(required = false) Integer size,
            @Parameter(description = "Campo de ordenação da entidade InvoiceEntity.")
            @RequestParam(defaultValue = "createdAt") String sort,
            @Parameter(description = "Direção da ordenação.")
            @RequestParam(defaultValue = "DESC") Sort.Direction direction
    ) {
        return ResponseEntity.ok(invoiceRepository.findAll(toPageable(page, size, sort, direction)));
    }

    private Pageable toPageable(Integer page, Integer size, String sort, Sort.Direction direction) {
        Sort sortSpec = Sort.by(direction, sort);
        if (page == null && size == null) {
            return Pageable.unpaged(sortSpec);
        }
        int pageNumber = page == null ? 0 : Math.max(page, 0);
        int pageSize = size == null ? 20 : Math.max(size, 1);
        return PageRequest.of(pageNumber, pageSize, sortSpec);
    }
}
