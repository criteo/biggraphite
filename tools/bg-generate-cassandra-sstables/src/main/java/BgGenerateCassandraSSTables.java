/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Inspired from https://github.com/yukim/cassandra-bulkload-example/
 */
package biggraphite;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import java.io.*;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.List;
import java.util.UUID;

/**
 * Usage: java biggraphite.BgGenerateCassandraSSTables
 */
public class BgGenerateCassandraSSTables
{
    /** Default output directory */
    public static final String DEFAULT_OUTPUT_DIR = "./data";

    /**
     * INSERT statement to bulk load.
     * It is like prepared statement. You fill in place holder for each data.
     */
    public static final String INSERT_STMT = "INSERT INTO %s.%s " +
           "(metric, time_start_ms, offset, value, count)" +
            " VALUES (?, ?, ?, ?, ?);";

    public static void main(String[] args) throws IOException {
        if (args.length != 4)
        {
            System.out.println("usage: java biggraphite.BgGenerateCassandraSSTables <KEYSPACE> <TABLE> <CQL> <CSV>");
            return;
        }
        final String keyspace = args[0];
        final String table = args[1];
        final String schema = new String(Files.readAllBytes(Paths.get(args[2])), StandardCharsets.UTF_8);
        final String data = args[3];
        final String insert_stmt = String.format(INSERT_STMT, keyspace, table);

        // magic!
        Config.setClientMode(true);

        // Create output directory that has keyspace and table name in the path
        File outputDir = new File(DEFAULT_OUTPUT_DIR + File.separator + keyspace + File.separator + table);
        if (!outputDir.exists() && !outputDir.mkdirs())
        {
            throw new RuntimeException("Cannot create output directory: " + outputDir);
        }

        // Prepare SSTable writer
        CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
        // set output directory
        builder.inDirectory(outputDir)
                .forTable(schema)
                .using(insert_stmt)
                .withPartitioner(new Murmur3Partitioner());
        CQLSSTableWriter writer = builder.build();

        try (
                BufferedReader reader = new BufferedReader(new FileReader(data));
                CsvListReader csvReader = new CsvListReader(reader, CsvPreference.STANDARD_PREFERENCE)
        )
        {
            csvReader.getHeader(true);

            // Write to SSTable while reading data
            List<String> line;
            while ((line = csvReader.read()) != null)
            {
                // We use Java types here based on
                // http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/DataType.Name.html#asJavaClass%28%29
                writer.addRow(
                        UUID.fromString(line.get(0)),          // metric uuid
                        Long.parseLong(line.get(1)),    // time_start_ms
                        Short.parseShort(line.get(2)),  // offset
                        Double.parseDouble(line.get(3)),    // value
                        Integer.parseInt(line.get(4))); // count
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        try
        {
            writer.close();
        }
        catch (IOException ignore) {}
    }
}