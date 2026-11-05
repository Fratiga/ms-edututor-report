package cl.duoc.edututorreport;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

// Alerta que publica ms-edututor-audit en sessions.alerts. alertId es el eventId
// del evento que disparó la regla: sirve para reconocer una reentrega y no
// guardar dos veces la misma alerta.
@JsonIgnoreProperties(ignoreUnknown = true)
public record SessionAlertDto(
	@NotBlank(message = "alertId es obligatorio") String alertId,
	@NotBlank(message = "regla es obligatoria") String regla,
	@NotBlank(message = "severidad es obligatoria") String severidad,
	@NotBlank(message = "estudianteId es obligatorio") String estudianteId,
	Long sessionId,
	@NotNull(message = "valor es obligatorio") Long valor,
	@NotNull(message = "umbral es obligatorio") Long umbral,
	Integer ventanaDias,
	String mensaje,
	@NotNull(message = "detectadaEn es obligatorio") Instant detectadaEn
) {
}
