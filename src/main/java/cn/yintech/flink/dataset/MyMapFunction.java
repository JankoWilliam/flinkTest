package cn.yintech.flink.dataset;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;

import java.util.List;

public class MyMapFunction<T> implements CheckpointedFunction, MapFunction<T, T> {


    @Override
    public T map(T t) throws Exception {
        return null;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }
}
