package cl.duoc.edututorreport;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface MetricaHoraRepository extends JpaRepository<MetricaHora, Long> {

	Optional<MetricaHora> findByHoraBucket(Instant horaBucket);

	@Query("SELECT m FROM MetricaHora m WHERE m.horaBucket >= :desde ORDER BY m.horaBucket ASC")
	List<MetricaHora> desde(@Param("desde") Instant desde);
}
