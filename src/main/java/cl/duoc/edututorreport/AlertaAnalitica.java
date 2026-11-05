package cl.duoc.edututorreport;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;

// Alerta de analítica recibida de sessions.alerts: un patrón detectado sobre el
// historial (por ejemplo, un estudiante que cancela con frecuencia).
@Entity
public class AlertaAnalitica {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	// eventId del evento que la disparó: evita guardar dos veces la misma alerta
	// si Kafka la entrega de nuevo.
	@Column(unique = true, nullable = false)
	private String alertId;

	private String regla;

	private String severidad;

	private String estudianteId;

	private Long sessionId;

	private Long valor;

	private Long umbral;

	private Integer ventanaDias;

	@Column(length = 500)
	private String mensaje;

	private Instant detectadaEn;

	private Instant registradaEn;

	// Posición del mensaje en sessions.alerts (trazabilidad).
	private Integer kafkaParticion;

	private Long kafkaOffset;

	protected AlertaAnalitica() {
	}

	public AlertaAnalitica(SessionAlertDto alerta, int kafkaParticion, long kafkaOffset) {
		this.alertId = alerta.alertId();
		this.regla = alerta.regla();
		this.severidad = alerta.severidad();
		this.estudianteId = alerta.estudianteId();
		this.sessionId = alerta.sessionId();
		this.valor = alerta.valor();
		this.umbral = alerta.umbral();
		this.ventanaDias = alerta.ventanaDias();
		this.mensaje = alerta.mensaje();
		this.detectadaEn = alerta.detectadaEn();
		this.registradaEn = Instant.now();
		this.kafkaParticion = kafkaParticion;
		this.kafkaOffset = kafkaOffset;
	}

	public Long getId() {
		return id;
	}

	public String getAlertId() {
		return alertId;
	}

	public String getRegla() {
		return regla;
	}

	public String getSeveridad() {
		return severidad;
	}

	public String getEstudianteId() {
		return estudianteId;
	}

	public Long getSessionId() {
		return sessionId;
	}

	public Long getValor() {
		return valor;
	}

	public Long getUmbral() {
		return umbral;
	}

	public Integer getVentanaDias() {
		return ventanaDias;
	}

	public String getMensaje() {
		return mensaje;
	}

	public Instant getDetectadaEn() {
		return detectadaEn;
	}

	public Instant getRegistradaEn() {
		return registradaEn;
	}

	public Integer getKafkaParticion() {
		return kafkaParticion;
	}

	public Long getKafkaOffset() {
		return kafkaOffset;
	}
}
