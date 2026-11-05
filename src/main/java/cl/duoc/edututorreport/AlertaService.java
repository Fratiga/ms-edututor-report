package cl.duoc.edututorreport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

// Guarda las alertas recibidas de sessions.alerts, una sola vez cada una.
@Service
public class AlertaService {

	private static final Logger log = LoggerFactory.getLogger(AlertaService.class);

	private final AlertaAnaliticaRepository repository;

	public AlertaService(AlertaAnaliticaRepository repository) {
		this.repository = repository;
	}

	public void registrar(SessionAlertDto alerta, int particion, long offset) {
		// Idempotencia: una alerta reentregada no se guarda dos veces.
		if (repository.findByAlertId(alerta.alertId()).isPresent()) {
			log.info("Alerta {} ya registrada (kafka p{} o{}), se ignora", alerta.alertId(), particion, offset);
			return;
		}
		repository.save(new AlertaAnalitica(alerta, particion, offset));
		log.info("Alerta registrada kafka p{} o{} {} severidad={} estudiante={} valor={}", particion, offset,
			alerta.regla(), alerta.severidad(), alerta.estudianteId(), alerta.valor());
	}
}
