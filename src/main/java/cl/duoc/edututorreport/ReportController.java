package cl.duoc.edututorreport;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;

@RestController
@RequestMapping("/api/report")
public class ReportController {

	private static final Pattern RANGE = Pattern.compile("last(\\d+)([hd])");

	private final MetricaHoraRepository metricaHoraRepo;
	private final MetricaServicioDiaRepository metricaServicioRepo;
	private final EstadoGaugeRepository gaugeRepo;

	public ReportController(MetricaHoraRepository metricaHoraRepo, MetricaServicioDiaRepository metricaServicioRepo,
			EstadoGaugeRepository gaugeRepo) {
		this.metricaHoraRepo = metricaHoraRepo;
		this.metricaServicioRepo = metricaServicioRepo;
		this.gaugeRepo = gaugeRepo;
	}

	// GET /api/report/kpis?range=last24h
	@GetMapping("/kpis")
	public Map<String, Object> kpis(@RequestParam(defaultValue = "last24h") String range) {
		Instant desde = Instant.now().minus(parseHoras(range), ChronoUnit.HOURS);
		List<MetricaHora> horas = metricaHoraRepo.desde(desde);

		long creadas = horas.stream().mapToLong(MetricaHora::getSesionesCreadas).sum();
		long realizadas = horas.stream().mapToLong(MetricaHora::getSesionesRealizadas).sum();
		long canceladas = horas.stream().mapToLong(MetricaHora::getSesionesCanceladas).sum();
		double tasaAsistencia = creadas == 0 ? 0.0 : (double) realizadas / creadas;

		Map<String, Long> estadosActivos = gaugeRepo.findAll().stream()
			.collect(Collectors.toMap(EstadoGauge::getEstado, EstadoGauge::getCantidad));

		return Map.of(
			"range", range,
			"sesionesPorHora", horas.stream().map(h -> Map.of(
				"hora", h.getHoraBucket().toString(),
				"creadas", h.getSesionesCreadas(),
				"realizadas", h.getSesionesRealizadas(),
				"canceladas", h.getSesionesCanceladas()
			)).toList(),
			"tasaAsistencia", tasaAsistencia,
			"estadosActivos", estadosActivos
		);
	}

	// GET /api/report/top-services?range=last7d
	@GetMapping("/top-services")
	public List<Map<String, Object>> topServices(@RequestParam(defaultValue = "last7d") String range) {
		LocalDate desde = Instant.now().minus(parseHoras(range), ChronoUnit.HOURS).atZone(ZoneOffset.UTC).toLocalDate();
		return metricaServicioRepo.topDesde(desde).stream()
			.map(r -> Map.<String, Object>of("servicioId", r.getServicioId(), "totalSolicitudes", r.getTotal()))
			.toList();
	}

	private long parseHoras(String range) {
		Matcher m = RANGE.matcher(range);
		if (!m.matches()) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
				"Formato de range inválido, use lastNh o lastNd (ej. last24h, last7d): " + range);
		}
		long cantidad = Long.parseLong(m.group(1));
		return m.group(2).equals("d") ? cantidad * 24 : cantidad;
	}
}
