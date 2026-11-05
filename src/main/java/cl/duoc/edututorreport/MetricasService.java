package cl.duoc.edututorreport;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

// Lado Query de CQRS: mantiene las tablas de agregados a partir de los eventos
// de sesiones, sin tocar jamás el esquema transaccional de sessions/catalog.
// Es lógica de negocio pura: no sabe de Kafka (el listener solo le entrega el DTO).
@Service
public class MetricasService {

	private static final Logger log = LoggerFactory.getLogger(MetricasService.class);

	private final ProcessedEventRepository processedEvents;
	private final MetricaHoraRepository metricaHoraRepo;
	private final MetricaServicioDiaRepository metricaServicioRepo;
	private final EstadoGaugeRepository gaugeRepo;

	public MetricasService(ProcessedEventRepository processedEvents, MetricaHoraRepository metricaHoraRepo,
			MetricaServicioDiaRepository metricaServicioRepo, EstadoGaugeRepository gaugeRepo) {
		this.processedEvents = processedEvents;
		this.metricaHoraRepo = metricaHoraRepo;
		this.metricaServicioRepo = metricaServicioRepo;
		this.gaugeRepo = gaugeRepo;
	}

	@Transactional
	public void aplicar(SessionEventDto evento, int particion, long offset) {
		// Idempotencia: Kafka entrega "al menos una vez"; un evento ya aplicado no suma dos veces.
		if (processedEvents.existsById(evento.eventId())) {
			log.info("Evento {} ya aplicado a las métricas (kafka p{} o{}), se ignora", evento.eventId(), particion, offset);
			return;
		}

		SessionEventDto.Payload payload = evento.payload();
		Instant ocurridoEn = evento.timestamp();
		MetricaHora hora = horaBucketDe(ocurridoEn);

		if (SessionEvent.CREATED.equals(evento.type())) {
			hora.incrementarCreadas();
			incrementarGauge(payload.estadoNuevo());
			incrementarServicioDia(payload.servicioId(), ocurridoEn);
		}
		else if (SessionEvent.STATE_CHANGED.equals(evento.type())) {
			if (payload.estadoAnterior() != null) {
				decrementarGauge(payload.estadoAnterior());
			}
			incrementarGauge(payload.estadoNuevo());
			if ("REALIZADA".equals(payload.estadoNuevo())) {
				hora.incrementarRealizadas();
			}
			else if ("CANCELADA".equals(payload.estadoNuevo())) {
				hora.incrementarCanceladas();
			}
		}

		metricaHoraRepo.save(hora);
		processedEvents.save(new ProcessedEvent(evento.eventId()));
		log.info("Aplicado kafka p{} o{} eventId={} tipo={} a las métricas", particion, offset, evento.eventId(),
			evento.type());
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
}
