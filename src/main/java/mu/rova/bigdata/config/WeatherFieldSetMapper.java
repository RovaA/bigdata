package mu.rova.bigdata.config;

import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

import mu.rova.bigdata.domain.Weather;

public class WeatherFieldSetMapper implements FieldSetMapper<Weather> {

	@Override
	public Weather mapFieldSet(FieldSet fieldSet) throws BindException {
		return new Weather(fieldSet.readString("country"), fieldSet.readString("rank"));
	}

}
