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
 */
import static org.junit.Assert.*;
import biggraphite.*;

public class BgGenerateCassandraSSTablesTest {
    @org.junit.Test
    public void mainShouldRun() throws Exception {
        // Just test that we can import simple files.
        String[] args = new String[]{
            "biggraphite", "datapoints",
            "src/test/resources/schema.cql",
            "src/test/resources/points.csv"};
        BgGenerateCassandraSSTables.main(args);
    }
}