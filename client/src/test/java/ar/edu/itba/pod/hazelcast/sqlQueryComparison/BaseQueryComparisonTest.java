package ar.edu.itba.pod.hazelcast.sqlQueryComparison;

// Assuming your ZonesRow is in this package
import ar.edu.itba.pod.hazelcast.common.ZonesRow;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.fail;


public abstract class BaseQueryComparisonTest {

    private static final Logger logger = LoggerFactory.getLogger(BaseQueryComparisonTest.class);
    protected static final String TRIPS_RESOURCE_PATH = "/trips-test.csv";
    protected static final String ZONES_RESOURCE_PATH = "/zones.csv";
    protected static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    protected static final int OUTSIDE_NYC_ID = 265;

    protected static Connection h2Connection;
    protected static TestHazelcastInstanceFactory testHazelcastInstanceFactory;
    protected static HazelcastInstance hazelcastInstance;
    protected static Map<Integer, ZonesRow> zonesMap;

    @BeforeAll
    public static void setUp() throws URISyntaxException, SQLException, IOException {
        String jdbcUrl = "jdbc:h2:mem:testdb";
        logger.info("Initiating H2 in-memory database connection.");

        h2Connection = DriverManager.getConnection(jdbcUrl);
        logger.info("Connected to H2 database.");

        URL tripsUrl = BaseQueryComparisonTest.class.getResource(TRIPS_RESOURCE_PATH);
        URL zonesUrl = BaseQueryComparisonTest.class.getResource(ZONES_RESOURCE_PATH);
        if (tripsUrl == null || zonesUrl == null) {
            fail("Test resource CSV files not found in classpath.");
        }
        String tripsPath = new File(tripsUrl.toURI()).getAbsolutePath();
        String zonesPath = new File(zonesUrl.toURI()).getAbsolutePath();

        try (Statement stmt = h2Connection.createStatement()) {
            String sql1 = String.format("CREATE TABLE trips_data AS SELECT * FROM CSVREAD('%s', null, 'fieldSeparator=;');", tripsPath);
            String sql2 = String.format("CREATE TABLE zones_data AS SELECT * FROM CSVREAD('%s', null, 'fieldSeparator=;');", zonesPath);
            stmt.execute(sql1);
            stmt.execute(sql2);
            logger.info("Data loaded into H2 from CSV files.");
        }

        logger.info("Starting in-memory Hazelcast cluster for testing.");
        testHazelcastInstanceFactory = new TestHazelcastInstanceFactory();
        hazelcastInstance = testHazelcastInstanceFactory.newHazelcastInstance();
        logger.info("Hazelcast cluster created.");

        logger.info("Loading zones data into memory...");
        zonesMap = new HashMap<>();
        loadZonesData(zonesPath);
        logger.info("Loaded {} zones.", zonesMap.size());
    }

    @AfterAll
    public static void tearDown() throws SQLException {
        if (h2Connection != null && !h2Connection.isClosed()) {
            h2Connection.close();
            logger.info("Closed connection to H2 database.");
        }
        logger.info("Shutting down Hazelcast cluster.");
        testHazelcastInstanceFactory.shutdownAll();
        logger.info("Hazelcast cluster shut down.");

        zonesMap.clear();
    }


    private static void loadZonesData(String zonesPath) throws IOException {
        try (Stream<String> lines = Files.lines(Paths.get(zonesPath), StandardCharsets.UTF_8)) {
            lines.skip(1)
                    .map(line -> line.split(";"))
                    .map(line -> new ZonesRow(
                            Integer.parseInt(line[0]),
                            line[1],
                            line[2]
                    ))
                    .forEach(zone -> zonesMap.put(zone.getLocationID(), zone));
        }
    }
}