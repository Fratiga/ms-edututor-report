package cl.duoc.edututorreport;

import java.util.List;
import java.util.Optional;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface AlertaAnaliticaRepository extends JpaRepository<AlertaAnalitica, Long> {

	Optional<AlertaAnalitica> findByAlertId(String alertId);

	// Las más recientes primero (la paginación acota cuántas se devuelven).
	@Query("SELECT a FROM AlertaAnalitica a ORDER BY a.detectadaEn DESC, a.id DESC")
	List<AlertaAnalitica> recientes(Pageable pagina);
}
