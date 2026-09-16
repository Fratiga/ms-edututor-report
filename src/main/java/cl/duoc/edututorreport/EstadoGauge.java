package cl.duoc.edututorreport;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

// Contador en vivo de sesiones actualmente en cada estado — se incrementa al
// entrar a un estado y se decrementa al salir de él, así "estados activos"
// siempre refleja el presente sin recorrer el historial completo.
@Entity
public class EstadoGauge {

	@Id
	private String estado;

	private long cantidad;

	protected EstadoGauge() {
	}

	public EstadoGauge(String estado) {
		this.estado = estado;
	}

	public String getEstado() {
		return estado;
	}

	public long getCantidad() {
		return cantidad;
	}

	public void incrementar() {
		this.cantidad++;
	}

	public void decrementar() {
		if (this.cantidad > 0) {
			this.cantidad--;
		}
	}
}
