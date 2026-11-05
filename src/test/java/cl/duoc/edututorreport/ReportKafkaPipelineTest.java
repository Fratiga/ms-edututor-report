package cl.duoc.edututorreport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.validation.autoconfigure.ValidationAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

// Los dos consumidores de report contra un broker Kafka real y con la MISMA
// configuración de application.properties: grupo de eventos, grupo de alertas,
// deserializer tolerante a errores, reintentos y DLT.
@SpringBootTest(classes = ReportKafkaPipelineTest.Config.class, properties = {
	"edututor.kafka.retry.initial-interval-ms=100",
	"edututor.kafka.retry.max-interval-ms=400"
})
class ReportKafkaPipelineTest {

	private static final String EVENTOS = "sessions.events";
	private static final String ALERTAS = "sessions.alerts";
	private static final String DLT_EVENTOS = "sessions.events.DLT";
	private static final String DLT_ALERTAS = "sessions.alerts.DLT";

	private static final EmbeddedKafkaKraftBroker BROKER =
		new EmbeddedKafkaKraftBroker(1, 3, EVENTOS, ALERTAS, DLT_EVENTOS, DLT_ALERTAS);
	private static final ProcessedEventRepository PROCESADOS = mock(ProcessedEventRepository.class);
	private static final MetricaHoraRepository HORAS = mock(MetricaHoraRepository.class);
	private static final MetricaServicioDiaRepository SERVICIOS = mock(MetricaServicioDiaRepository.class);
	private static final EstadoGaugeRepository GAUGES = mock(EstadoGaugeRepository.class);
	private static final AlertaAnaliticaRepository ALERTAS_REPO = mock(AlertaAnaliticaRepository.class);

	static {
		BROKER.afterPropertiesSet();
	}

	@DynamicPropertySource
	static void propiedades(DynamicPropertyRegistry registry) {
		registry.add("spring.kafka.bootstrap-servers", BROKER::getBrokersAsString);
	}

	@Configuration
	@ImportAutoConfiguration({ KafkaAutoConfiguration.class, ValidationAutoConfiguration.class })
	@Import({ KafkaClientConfig.class, ReportEventListener.class, AlertListener.class, EventValidator.class,
		MetricasService.class, AlertaService.class })
	static class Config {

		@Bean
		ProcessedEventRepository procesados() {
			return PROCESADOS;
		}

		@Bean
		MetricaHoraRepository horas() {
			return HORAS;
		}

		@Bean
		MetricaServicioDiaRepository servicios() {
			return SERVICIOS;
		}

		@Bean
		EstadoGaugeRepository gauges() {
			return GAUGES;
		}

		@Bean
		AlertaAnaliticaRepository alertas() {
			return ALERTAS_REPO;
		}
	}

	@Autowired
	KafkaTemplate<String, byte[]> bytes;

	private static String evento(String eventId, long sessionId, String estadoNuevo) {
		return ("{'eventId':'" + eventId + "','type':'SESSION_STATE_CHANGED','timestamp':'2026-11-05T12:00:00Z',"
			+ "'traceId':'t','correlationId':'c','payload':{'sessionId':" + sessionId
			+ ",'estudianteId':'STU-1','servicioId':2,'tutorId':null,'estadoAnterior':'CONFIRMADA','estadoNuevo':'"
			+ estadoNuevo + "'}}").replace('\'', '"');
	}

	private static String alerta(String alertId, String estudiante) {
		return ("{'alertId':'" + alertId + "','regla':'CANCELACIONES_REITERADAS','severidad':'MEDIA','estudianteId':'"
			+ estudiante + "','sessionId':5,'valor':3,'umbral':3,'ventanaDias':7,'mensaje':'3 cancelaciones',"
			+ "'detectadaEn':'2026-11-05T12:00:00Z'}").replace('\'', '"');
	}

	private SendResult<String, byte[]> enviar(String topico, String key, String valor) throws Exception {
		return bytes.send(topico, key, valor.getBytes(StandardCharsets.UTF_8)).get();
	}

	private static Optional<ConsumerRecord<String, String>> buscar(String topico,
			Predicate<ConsumerRecord<String, String>> condicion) {
		try (KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(Map.of(
				ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER.getBrokersAsString(),
				ConsumerConfig.GROUP_ID_CONFIG, "prueba-" + UUID.randomUUID(),
				ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
				ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
				ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
			consumidor.subscribe(List.of(topico));
			long limite = System.currentTimeMillis() + 25_000;
			while (System.currentTimeMillis() < limite) {
				for (ConsumerRecord<String, String> registro : consumidor.poll(Duration.ofMillis(500))) {
					if (condicion.test(registro)) {
						return Optional.of(registro);
					}
				}
			}
		}
		return Optional.empty();
	}

	private static String cabecera(ConsumerRecord<String, String> registro, String nombre) {
		Header h = registro.headers().lastHeader(nombre);
		return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
	}

	@Test
	void unEventoValidoActualizaLasMetricas() throws Exception {
		when(PROCESADOS.existsById("m-1")).thenReturn(false);
		when(HORAS.findByHoraBucket(any())).thenReturn(Optional.empty());
		when(GAUGES.findById(any())).thenReturn(Optional.empty());

		enviar(EVENTOS, "20", evento("m-1", 20, "CANCELADA"));

		verify(PROCESADOS, timeout(20_000)).save(argThat(p -> p != null));
		verify(HORAS, timeout(20_000)).save(argThat(h -> h != null && h.getSesionesCanceladas() == 1));
	}

	@Test
	void unaAlertaValidaSeGuardaDesdeElSegundoTopico() throws Exception {
		when(ALERTAS_REPO.findByAlertId("a-1")).thenReturn(Optional.empty());

		SendResult<String, byte[]> enviado = enviar(ALERTAS, "STU-9:CANCELACIONES_REITERADAS", alerta("a-1", "STU-9"));

		var guardada = org.mockito.ArgumentCaptor.forClass(AlertaAnalitica.class);
		verify(ALERTAS_REPO, timeout(20_000)).save(guardada.capture());
		assertThat(guardada.getValue().getAlertId()).isEqualTo("a-1");
		assertThat(guardada.getValue().getEstudianteId()).isEqualTo("STU-9");
		assertThat(guardada.getValue().getKafkaParticion()).isEqualTo(enviado.getRecordMetadata().partition());
		assertThat(guardada.getValue().getKafkaOffset()).isEqualTo(enviado.getRecordMetadata().offset());
	}

	@Test
	void unaAlertaReentregadaNoSeGuardaDosVeces() throws Exception {
		when(ALERTAS_REPO.findByAlertId("a-dup")).thenReturn(Optional.of(mock(AlertaAnalitica.class)));
		when(ALERTAS_REPO.findByAlertId("a-sig")).thenReturn(Optional.empty());

		enviar(ALERTAS, "k", alerta("a-dup", "STU-1"));
		enviar(ALERTAS, "k", alerta("a-sig", "STU-1"));

		// al llegar la siguiente, la duplicada ya fue tratada (mismo hilo y partición no garantizada: se espera a la señal)
		verify(ALERTAS_REPO, timeout(20_000)).save(argThat(a -> a != null && "a-sig".equals(a.getAlertId())));
		verify(ALERTAS_REPO, org.mockito.Mockito.never()).save(argThat(a -> a != null && "a-dup".equals(a.getAlertId())));
	}

	@Test
	void unTextoQueNoEsJsonEnEventosVaALaDlt() throws Exception {
		enviar(EVENTOS, null, "hola");

		var enDlt = buscar(DLT_EVENTOS, r -> "hola".equals(r.value()));
		assertThat(enDlt).as("«hola» debe estar en " + DLT_EVENTOS).isPresent();
		assertThat(cabecera(enDlt.get(), "kafka_dlt-original-topic")).isEqualTo(EVENTOS);
	}

	@Test
	void unaAlertaInvalidaVaALaDltDeAlertasSinGuardarse() throws Exception {
		String sinRegla = alerta("a-mala", "STU-1").replace("\"regla\":\"CANCELACIONES_REITERADAS\",", "");

		enviar(ALERTAS, "k", sinRegla);

		var enDlt = buscar(DLT_ALERTAS, r -> r.value() != null && r.value().contains("a-mala"));
		assertThat(enDlt).as("la alerta inválida debe estar en " + DLT_ALERTAS).isPresent();
		assertThat(cabecera(enDlt.get(), "kafka_dlt-exception-message")).contains("regla");
		verify(ALERTAS_REPO, org.mockito.Mockito.never()).save(argThat(a -> a != null && "a-mala".equals(a.getAlertId())));
	}
}
