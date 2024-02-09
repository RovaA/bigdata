package mu.rova.bigdata.config;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.item.ItemReader;
import org.springframework.core.io.ClassPathResource;

import mu.rova.bigdata.domain.Weather;

public class WeatherChunkItemReader implements ItemReader<List<Weather>> {
	
	private List<Weather> weathers;
	
	private int currentChunk = 0;
	
	private int batchSize = 10;
	
	private final String COMMA_DELIMITER = ";";

	public WeatherChunkItemReader(String filePath, Long batchSize) {
		this.batchSize = (int) (long) batchSize;
		 weathers = new ArrayList<>();
		try (BufferedReader br = new BufferedReader(new FileReader(new ClassPathResource(filePath).getFile()))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		        String[] values = line.split(COMMA_DELIMITER);
		        weathers.add(new Weather(values[0], values[1]));
		    }
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public List<Weather> read() {

		int chunkMax = Math.round(weathers.size() / batchSize);
		
		if (currentChunk > chunkMax) {
			return null;
		}
		int minRange = currentChunk * batchSize;
		int maxRange = minRange + batchSize;
		List<Weather> weathersChunk = weathers.subList(minRange, Math.min(weathers.size(), maxRange));
		currentChunk++;
		return weathersChunk;
	}

}
