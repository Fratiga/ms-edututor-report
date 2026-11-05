package cl.duoc.edututorreport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

// La lógica de negocio de las métricas, sin Kafka: el servicio solo recibe el DTO.
class MetricasServiceTest {

	private static final Instant AHORA = Instant.parse("2026-11-05T12:34:56Z");

	private ProcessedEventRepository procesados;
	private MetricaHoraRepository horaRepo;
	private MetricaServicioDiaRepository servicioRepo;
	private EstadoGaugeRepository gaugeRepo;
	private MetricasService servicio;

	@BeforeEach
	void preparar() {
		procesados = mock(ProcessedEventRepository.class);
		horaRepo = mock(MetricaHoraRepository.class);
		servicioRepo = mock(MetricaServicioDiaRepository.class);
		gaugeRepo = mock(EstadoGaugeRepository.class);
		when(horaRepo.findByHoraBucket(any())).thenReturn(Optional.empty());
		when(servicioRepo.findByServicioIdAndDia(any(), any())).thenReturn(Optional.empty());
		when(gaugeRepo.findById(any())).thenReturn(Optional.empty());
		servicio = new MetricasService(procesados, horaRepo, servicioRepo, gaugeRepo);
	}

	private static SessionEventDto evento(String id, String tipo, String anterior, String nuevo) {
		return new SessionEventDto(id, tipo, AHORA, "t", "c",
			new SessionEventDto.Payload(1L, "STU-1", 2L, null, anterior, nuevo));
	}

	@Test
	void unaSesionNuevaSumaCreadasYElEstadoInicial() {
		servicio.aplicar(evento("e1", SessionEvent.CREATED, null, "SOLICITADA"), 0, 5);

		var hora = org.mockito.ArgumentCaptor.forClass(MetricaHora.class);
		verify(horaRepo).save(hora.capture());
		assertThat(hora.getValue().getSesionesCreadas()).isEqualTo(1);
		assertThat(hora.getValue().getHoraBucket()).isEqualTo(Instant.parse("2026-11-05T12:00:00Z"));
		verify(servicioRepo).save(any(MetricaServicioDia.class));
		verify(gaugeRepo).save(any(EstadoGauge.class));
		verify(procesados).save(any(ProcessedEvent.class));
	}

	@Test
	void unaCancelacionSumaCanceladas() {
		servicio.aplicar(evento("e2", SessionEvent.STATE_CHANGED, "CONFIRMADA", "CANCELADA"), 1, 9);

		var hora = org.mockito.ArgumentCaptor.forClass(MetricaHora.class);
		verify(horaRepo).save(hora.capture());
		assertThat(hora.getValue().getSesionesCanceladas()).isEqualTo(1);
		assertThat(hora.getValue().getSesionesRealizadas()).isZero();
	}

	@Test
	void unaSesionRealizadaSumaRealizadas() {
		servicio.aplicar(evento("e3", SessionEvent.STATE_CHANGED, "CONFIRMADA", "REALIZADA"), 1, 10);

		var hora = org.mockito.ArgumentCaptor.forClass(MetricaHora.class);
		verify(horaRepo).save(hora.capture());
		assertThat(hora.getValue().getSesionesRealizadas()).isEqualTo(1);
	}

	@Test
	void unEventoYaAplicadoNoSumaDosVeces() {
		when(procesados.existsById("e4")).thenReturn(true);

		servicio.aplicar(evento("e4", SessionEvent.CREATED, null, "SOLICITADA"), 0, 11);

		verify(horaRepo, never()).save(any());
		verify(procesados, never()).save(any());
	}

	@Test
	void elCambioDeEstadoMueveElContadorEnVivo() {
		var anterior = new EstadoGauge("CONFIRMADA");
		anterior.incrementar();
		when(gaugeRepo.findById("CONFIRMADA")).thenReturn(Optional.of(anterior));

		servicio.aplicar(evento("e5", SessionEvent.STATE_CHANGED, "CONFIRMADA", "REALIZADA"), 0, 12);

		assertThat(anterior.getCantidad()).isZero();
		verify(gaugeRepo).save(anterior);
	}
}
