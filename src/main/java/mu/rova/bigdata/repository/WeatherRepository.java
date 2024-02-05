package mu.rova.bigdata.repository;

import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.stereotype.Repository;

import mu.rova.bigdata.domain.Weather;

@Repository
public interface WeatherRepository extends R2dbcRepository<Weather, Long> {

}
