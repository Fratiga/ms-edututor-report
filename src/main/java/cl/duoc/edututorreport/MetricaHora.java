package cl.duoc.edututorreport;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;

// Tabla de agregados pre-calculada (lado Query de CQRS): una fila por hora,
// incrementada de forma incremental al llegar cada evento — nunca se
// recalcula desde cero ni se toca el esquema transaccional de sessions.
@Entity
public class MetricaHora {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(unique = true)
	private Instant horaBucket;

	private long sesionesCreadas;
	private long sesionesRealizadas;
	private long sesionesCanceladas;

	protected MetricaHora() {
	}

	public MetricaHora(Instant horaBucket) {
		this.horaBucket = horaBucket;
	}

	public Instant getHoraBucket() {
		return horaBucket;
	}

	public long getSesionesCreadas() {
		return sesionesCreadas;
	}

	public void incrementarCreadas() {
		this.sesionesCreadas++;
	}

	public long getSesionesRealizadas() {
		return sesionesRealizadas;
	}

	public void incrementarRealizadas() {
		this.sesionesRealizadas++;
	}

	public long getSesionesCanceladas() {
		return sesionesCanceladas;
	}

	public void incrementarCanceladas() {
		this.sesionesCanceladas++;
	}
}
