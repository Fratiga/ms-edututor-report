package cl.duoc.edututorreport;

// Tipos de evento que publica ms-edututor-sessions (el campo "type" del mensaje).
public final class SessionEvent {

	public static final String CREATED = "SESSION_CREATED";
	public static final String STATE_CHANGED = "SESSION_STATE_CHANGED";

	private SessionEvent() {
	}
}
