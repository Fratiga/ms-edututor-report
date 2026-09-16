package cl.duoc.edututorreport;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface MetricaServicioDiaRepository extends JpaRepository<MetricaServicioDia, Long> {

	Optional<MetricaServicioDia> findByServicioIdAndDia(Long servicioId, LocalDate dia);

	@Query("""
		SELECT m.servicioId as servicioId, SUM(m.totalSolicitudes) as total
		FROM MetricaServicioDia m
		WHERE m.dia >= :desde
		GROUP BY m.servicioId
		ORDER BY total DESC
		""")
	List<TopServicio> topDesde(@Param("desde") LocalDate desde);

	interface TopServicio {
		Long getServicioId();
		Long getTotal();
	}
}
