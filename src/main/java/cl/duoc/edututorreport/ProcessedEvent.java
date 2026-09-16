package cl.duoc.edututorreport;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

// Idempotencia: evita aplicar dos veces el mismo evento a los agregados
// (semántica at-least-once de Kafka).
@Entity
public class ProcessedEvent {

	@Id
	private String eventId;

	protected ProcessedEvent() {
	}

	public ProcessedEvent(String eventId) {
		this.eventId = eventId;
	}
}
