package cl.duoc.edututorreport;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

// Capa de mensajería de sessions.alerts (el segundo tópico): recibe la alerta
// que publica audit, la valida y la guarda a través de AlertaService.
//
// Grupo report-alerts-v1, distinto del de eventos: cada flujo lleva su propio
// avance y se puede reposicionar uno sin tocar el otro. El tipo del mensaje
// (SessionAlertDto) se fija solo para este listener; el del otro es SessionEventDto.
@Component
public class AlertListener {

	private final EventValidator validador;
	private final AlertaService alertas;

	public AlertListener(EventValidator validador, AlertaService alertas) {
		this.validador = validador;
		this.alertas = alertas;
	}

	@KafkaListener(topics = "${edututor.kafka.topic.alerts}", groupId = "${edututor.kafka.alerts-group}",
			concurrency = "1",
			properties = "spring.json.value.default.type=cl.duoc.edututorreport.SessionAlertDto")
	public void onAlert(
			@Payload(required = false) SessionAlertDto alerta,
			@Header(KafkaHeaders.RECEIVED_PARTITION) int particion,
			@Header(KafkaHeaders.OFFSET) long offset) {
		validador.validar(alerta);
		alertas.registrar(alerta, particion, offset);
	}
}
