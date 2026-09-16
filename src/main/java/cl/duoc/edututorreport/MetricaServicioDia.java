package cl.duoc.edututorreport;

import java.time.LocalDate;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;

// "Asignaturas más solicitadas": una fila por servicio y día, para poder
// sumar sobre cualquier rango (last7d, last24h, etc.) sin recalcular todo.
@Entity
public class MetricaServicioDia {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	private Long servicioId;
	private LocalDate dia;
	private long totalSolicitudes;

	protected MetricaServicioDia() {
	}

	public MetricaServicioDia(Long servicioId, LocalDate dia) {
		this.servicioId = servicioId;
		this.dia = dia;
	}

	public Long getServicioId() {
		return servicioId;
	}

	public LocalDate getDia() {
		return dia;
	}

	public long getTotalSolicitudes() {
		return totalSolicitudes;
	}

	public void incrementar() {
		this.totalSolicitudes++;
	}
}
