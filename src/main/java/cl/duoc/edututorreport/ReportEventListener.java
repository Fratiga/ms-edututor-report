package cl.duoc.edututorreport;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

// Lado Query de CQRS: procesa sessions.events y mantiene tablas de
// agregados, sin tocar jamás el esquema transaccional de sessions/catalog.
@Component
public class ReportEventListener {

	private static final Logger log = LoggerFactory.getLogger(ReportEventListener.class);

	private final ProcessedEventRepository processedEvents;
	private final MetricaHoraRepository metricaHoraRepo;
	private final MetricaServicioDiaRepository metricaServicioRepo;
	private final EstadoGaugeRepository gaugeRepo;

	public ReportEventListener(ProcessedEventRepository processedEvents, MetricaHoraRepository metricaHoraRepo,
			MetricaServicioDiaRepository metricaServicioRepo, EstadoGaugeRepository gaugeRepo) {
		this.processedEvents = processedEvents;
		this.metricaHoraRepo = metricaHoraRepo;
		this.metricaServicioRepo = metricaServicioRepo;
		this.gaugeRepo = gaugeRepo;
	}

	@Transactional
	@KafkaListener(topics = "sessions.events", groupId = "report-service-v1")
	public void onSessionEvent(@Payload Map<String, Object> evento) {
		String eventId = (String) evento.get("eventId");
		if (processedEvents.existsById(eventId)) {
			log.info("Evento {} ya aplicado a las métricas, se ignora", eventId);
			return;
		}

		@SuppressWarnings("unchecked")
		Map<String, Object> payload = (Map<String, Object>) evento.get("payload");
		String tipo = (String) evento.get("type");
		Instant ocurridoEn = parseInstant(evento.get("timestamp"));
		Long servicioId = payload.get("servicioId") == null ? null : Long.valueOf(payload.get("servicioId").toString());
		String estadoNuevo = (String) payload.get("estadoNuevo");
		String estadoAnterior = (String) payload.get("estadoAnterior");

		MetricaHora hora = horaBucketDe(ocurridoEn);

		if ("SESSION_CREATED".equals(tipo)) {
			hora.incrementarCreadas();
			incrementarGauge(estadoNuevo);
			incrementarServicioDia(servicioId, ocurridoEn);
		} else if ("SESSION_STATE_CHANGED".equals(tipo)) {
			if (estadoAnterior != null) {
				decrementarGauge(estadoAnterior);
			}
			incrementarGauge(estadoNuevo);
			if ("REALIZADA".equals(estadoNuevo)) {
				hora.incrementarRealizadas();
			} else if ("CANCELADA".equals(estadoNuevo)) {
				hora.incrementarCanceladas();
			}
		}

		metricaHoraRepo.save(hora);
		processedEvents.save(new ProcessedEvent(eventId));
		log.info("Aplicado eventId={} tipo={} a las métricas", eventId, tipo);
	}

	private MetricaHora horaBucketDe(Instant instante) {
		Instant truncado = instante.truncatedTo(ChronoUnit.HOURS);
		return metricaHoraRepo.findByHoraBucket(truncado).orElseGet(() -> new MetricaHora(truncado));
	}

	private void incrementarServicioDia(Long servicioId, Instant instante) {
		if (servicioId == null) {
			return;
		}
		LocalDate dia = instante.atZone(ZoneOffset.UTC).toLocalDate();
		MetricaServicioDia metrica = metricaServicioRepo.findByServicioIdAndDia(servicioId, dia)
			.orElseGet(() -> new MetricaServicioDia(servicioId, dia));
		metrica.incrementar();
		metricaServicioRepo.save(metrica);
	}

	private void incrementarGauge(String estado) {
		if (estado == null) {
			return;
		}
		EstadoGauge gauge = gaugeRepo.findById(estado).orElseGet(() -> new EstadoGauge(estado));
		gauge.incrementar();
		gaugeRepo.save(gauge);
	}

	private void decrementarGauge(String estado) {
		gaugeRepo.findById(estado).ifPresent(g -> {
			g.decrementar();
			gaugeRepo.save(g);
		});
	}

	private Instant parseInstant(Object valor) {
		if (valor instanceof String s) {
			return Instant.parse(s);
		}
		if (valor instanceof Number n) {
			return Instant.ofEpochMilli(n.longValue());
		}
		throw new IllegalArgumentException("timestamp no reconocido: " + valor);
	}
}
