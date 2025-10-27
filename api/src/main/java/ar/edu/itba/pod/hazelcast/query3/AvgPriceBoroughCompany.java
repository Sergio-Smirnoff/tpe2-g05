package ar.edu.itba.pod.hazelcast.query3;

public record AvgPriceBoroughCompany(String borough, String company, Double avgPrice){
    @Override
    public String toString(){
        return "%s;%s;%.2f".formatted(borough,company,avgPrice);
    }
}
