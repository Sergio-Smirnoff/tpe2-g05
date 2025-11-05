//package ar.edu.itba.pod.hazelcast.client;
//
//
//import ar.edu.itba.pod.hazelcast.query3.*;
//import com.hazelcast.core.ICompletableFuture;
//import com.hazelcast.core.IMap;
//import com.hazelcast.mapreduce.Job;
//import com.hazelcast.mapreduce.JobTracker;
//import com.hazelcast.mapreduce.KeyValueSource;
//import ar.edu.itba.pod.hazelcast.common.*;
//
//import java.io.IOException;
//import java.nio.charset.StandardCharsets;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.time.LocalDateTime;
//import java.util.*;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.stream.Stream;
//
//public class ClientQuery3 extends Client<TripRowQ3, List<AvgPriceBoroughCompany>>{
//    private static final Integer QUERY_NUMBER = 3;
//
//    public ClientQuery3(final String address, final String inPath, final String outPath){
//        super(QUERY_NUMBER, address, inPath, outPath);
//    }
//
//    @Override
//    KeyValueSource<Integer, TripRowQ3> loadData() throws IOException {
//        this.loadZonesData();
//
//        // now loading the data
//        IMap<Integer, TripRowQ3> tripsMap = hazelcastInstance.getMap("trips-" + QUERY_NUMBER);// key is PULocationId
//        KeyValueSource<Integer, TripRowQ3> tripsKeyValueSource = KeyValueSource.fromMap(tripsMap);
//
//        final AtomicInteger tripsMapKey = new AtomicInteger();
//        try (Stream<String> lines = Files.lines(Path.of(inPath).resolve(TRIPS_PATH), StandardCharsets.UTF_8)) {
//            lines.parallel().skip(1)
//                    .map(line -> line.split(";"))
//                    .filter(line -> {
//                        ZonesRow pu = this.zonesMap.get(Integer.parseInt(line[4]));
//                        if ( pu == null )
//                            return false;
//                        String aux = pu.getZone();
//                        return !aux.equals("Outside of NYC");
//                    })
//                    .map(line -> new TripRowQ3(
//                            zonesMap.get(Integer.parseInt(line[4])).getBorough(), // borough
//                            line[0],
//                            Double.parseDouble(line[7])
//                    ))
//                    .forEach(trip -> {
//                        Integer uniqueId = tripsMapKey.getAndIncrement();
//                        tripsMap.put(uniqueId, trip);
//                    });
//        }
//
//        return tripsKeyValueSource;
//    }
//
//    @Override
//    ICompletableFuture<List<AvgPriceBoroughCompany>> executeMapReduce(JobTracker jobTracker, KeyValueSource<Integer, TripRowQ3> keyValueSource) {
//        return jobTracker.newJob(keyValueSource)
//                .mapper(new PriceAvgMapper())
//                .reducer(new PickupCompanyPairReducerFactory())
//                .submit(new Query3Collator());
//    }
//
//    @Override
//    void writeResults(List<AvgPriceBoroughCompany> results)  {
//        List<String> toPrint = new ArrayList<>();
//        toPrint.add("pickUpBorough;company;avgFare");
//        toPrint.addAll(results.stream().map(Objects::toString).toList());
//        this.printResults(toPrint);
//    }
//
//
//    public static void main(String[] args) {
//        ClientQuery3 clientQuery3 = new ClientQuery3(System.getProperty("addresses"), System.getProperty("inPath"), System.getProperty("outPath"));
//        clientQuery3.run();
//    }
//}
