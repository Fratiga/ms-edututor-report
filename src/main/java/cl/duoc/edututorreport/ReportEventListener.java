package cl.duoc.edututorreport;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

// Capa de mensajería de sessions.events: recibe el evento, lo valida y se lo
// entrega a MetricasService. Sin lógica de negocio.
//
// Grupo report-service-v1: lee TODOS los eventos, igual que audit con su propio
// grupo (cada grupo lleva su avance). Va con concurrencia 1: las métricas son
// contadores compartidos (por hora, por estado) y dos hilos actualizándolos a la
// vez perderían sumas.
//
// Un mensaje inválido o que sigue fallando tras los reintentos termina en
// sessions.events.DLT (ver KafkaClientConfig).
@Component
public class ReportEventListener {

	private final EventValidator validador;
	private final MetricasService metricas;

	public ReportEventListener(EventValidator validador, MetricasService metricas) {
		this.validador = validador;
		this.metricas = metricas;
	}

	@KafkaListener(topics = "${edututor.kafka.topic.events}", groupId = "${spring.kafka.consumer.group-id}",
			concurrency = "1")
	public void onSessionEvent(
			@Payload(required = false) SessionEventDto evento,
			@Header(KafkaHeaders.RECEIVED_PARTITION) int particion,
			@Header(KafkaHeaders.OFFSET) long offset) {
		validador.validar(evento);
		metricas.aplicar(evento, particion, offset);
	}
}
