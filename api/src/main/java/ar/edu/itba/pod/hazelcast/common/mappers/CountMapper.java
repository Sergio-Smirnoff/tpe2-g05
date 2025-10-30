package ar.edu.itba.pod.hazelcast.common.mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.function.Function;

public class CountMapper<VI, KO, VO> implements Mapper<Integer, VI, KO, VO> {
    private final Function<VI, KO> keyMapper;
    private final Function<VI, VO> valueMapper;

    public CountMapper(Function<VI, KO> keyMapper, Function<VI, VO> valueMapper) {
        this.keyMapper = keyMapper;
        this.valueMapper = valueMapper;
    }

    @Override
    public void map(Integer integer, VI vi, Context<KO, VO> context) {
        context.emit(keyMapper.apply(vi), valueMapper.apply(vi));
    }
}
