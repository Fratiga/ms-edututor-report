package cl.duoc.edututorreport;

import java.time.Instant;
import java.util.List;

import org.springframework.data.domain.PageRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/report/alerts")
public class AlertController {

	private static final int MAXIMO = 200;

	private final AlertaAnaliticaRepository repository;

	public AlertController(AlertaAnaliticaRepository repository) {
		this.repository = repository;
	}

	public record AlertaResponse(String id, String regla, String severidad, String estudianteId, Long sessionId,
			long valor, long umbral, Integer ventanaDias, String mensaje, Instant detectadaEn, Integer kafkaParticion,
			Long kafkaOffset) {
		static AlertaResponse from(AlertaAnalitica a) {
			return new AlertaResponse(a.getAlertId(), a.getRegla(), a.getSeveridad(), a.getEstudianteId(),
				a.getSessionId(), a.getValor() == null ? 0 : a.getValor(), a.getUmbral() == null ? 0 : a.getUmbral(),
				a.getVentanaDias(), a.getMensaje(), a.getDetectadaEn(), a.getKafkaParticion(), a.getKafkaOffset());
		}
	}

	// GET /api/report/alerts?limit=50: las alertas más recientes primero
	@GetMapping
	public List<AlertaResponse> recientes(@RequestParam(defaultValue = "50") int limit) {
		int cantidad = Math.max(1, Math.min(limit, MAXIMO));
		return repository.recientes(PageRequest.of(0, cantidad)).stream().map(AlertaResponse::from).toList();
	}
}
