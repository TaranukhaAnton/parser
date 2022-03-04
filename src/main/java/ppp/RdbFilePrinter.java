package ppp;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.whitbeck.rdbparser.Entry;
import net.whitbeck.rdbparser.KeyValuePair;
import net.whitbeck.rdbparser.RdbParser;
import net.whitbeck.rdbparser.SelectDb;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.springframework.data.redis.serializer.RedisSerializer;
import se.ticket.commons.dynamicpricing.MessagePackRedisSerializer;
import se.ticket.commons.dynamicpricing.PriceResult;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;

import static net.whitbeck.rdbparser.EntryType.KEY_VALUE_PAIR;
import static net.whitbeck.rdbparser.EntryType.SELECT_DB;

public class RdbFilePrinter {


	public static void main(String[] args) throws Exception {
		Options options = new Options();
		Option input = new Option("i", "input", true, "input file path");
		input.setRequired(true);
		options.addOption(input);

		Option output = new Option("o", "output", true, "output dir path");
		output.setRequired(true);
		options.addOption(output);

		Option partition = new Option("p", "partition", true, "how many records will contain each scv file");
		partition.setRequired(true);
		options.addOption(partition);

		CommandLineParser parser = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;//not a good practice, it serves it purpose

		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			System.out.println(e.getMessage());
			formatter.printHelp("rdb file transformer", options);

			System.exit(1);
		}

		String inputFilePath = cmd.getOptionValue("input");
		String outputDirPath = cmd.getOptionValue("output");
		String p = cmd.getOptionValue("partition");

		System.out.println(inputFilePath);
		System.out.println(outputDirPath);
		System.out.println(p);

		printRdbFile(cmd.getOptionValue("input"), cmd.getOptionValue("output"), Integer.parseInt(cmd.getOptionValue("partition")));
	}


	public static void printRdbFile(String i, String o, Integer p) throws Exception {
		ObjectMapper mapper = new ObjectMapper();

		try {
			FileUtils.cleanDirectory(new File(o));
		} catch (IllegalArgumentException e) {
			System.out.println(e.getMessage());
			new File(o).mkdir();
		}


		RedisSerializer keySerializer = new MessagePackRedisSerializer<>(PriceResult.class, true);
		int ii = 0;
		int fileNum = 0;


		RdbParser parser = null;
		FileWriter fw = null;

		try {
			parser = new RdbParser(new File(i));


			Entry e;
			while ((e = parser.readNext()) != null) {


				if (e.getType() == SELECT_DB) {
					System.out.println("Processing DB: " + ((SelectDb) e).getId());
					System.out.println("------------");
				}
				if (e.getType() == KEY_VALUE_PAIR) {
					if (ii % p == 0) {
						if (fw != null) {
							fw.close();
						}
						String fileName = o + "\\\\priceResults" + fileNum + ".csv";
						System.out.println("fileName = " + fileName);
						File out = new File(fileName);
						out.createNewFile();
						fw = new FileWriter(out);
						fileNum++;
					}
					KeyValuePair kvp = (KeyValuePair) e;
					String key = new String(kvp.getKey(), StandardCharsets.US_ASCII);
					String jsonValue = mapper.writeValueAsString( keySerializer.deserialize(kvp.getValues().get(0)));
					fw.write(key + "," + jsonValue + "\n");
					ii++;
				}
			}
		} finally {

			if (parser != null) {
				parser.close();
			}
			if (fw != null) {
				fw.close();
			}
		}


	}
}

