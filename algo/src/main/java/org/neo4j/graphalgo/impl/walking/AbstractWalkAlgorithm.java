package org.neo4j.graphalgo.impl.walking;

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.logging.Log;

import java.util.concurrent.*;

public abstract class AbstractWalkAlgorithm {

    private IdMapping idMapping;
    protected Log log;
    protected HeavyGraph graph;

    public AbstractWalkAlgorithm(HeavyGraph graph, Log log){
        this.idMapping = graph;
        this.graph = graph;
        this.log = log;
    }

    protected long getOriginalId(int nodeId) {
        return idMapping.toOriginalNodeId(nodeId);
    }

    protected int getMappedId(long nodeId) {
        return idMapping.toMappedNodeId(nodeId);
    }

    protected long[] translateIdsToOriginal(int[] mappedIds){
        long[] originalIds = new long[mappedIds.length];
        for(int i = 0; i < mappedIds.length; i++){
            originalIds[i] = getOriginalId(mappedIds[i]);
        }
        return originalIds;
    }

    protected ThreadPoolExecutor getExecutor(){
        int cores = Runtime.getRuntime().availableProcessors();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(cores * 4);
        executor.setCorePoolSize(cores * 4);

        return executor;
    }

    protected BoundedExecutor getBoundedExecutor(){
        ThreadPoolExecutor executor = getExecutor();
        return new BoundedExecutor(executor, executor.getCorePoolSize() * 1000);
    }

    public class BoundedExecutor {
        private final ExecutorService exec;
        private final Semaphore semaphore;

        public BoundedExecutor(ExecutorService exec, int bound) {
            this.exec = exec;
            this.semaphore = new Semaphore(bound);
        }

        public void submitTask(final Runnable command)
                throws InterruptedException, RejectedExecutionException {
            semaphore.acquire();
            try {
                exec.execute(new Runnable() {
                    public void run() {
                        try {
                            command.run();
                        } finally {
                            semaphore.release();
                        }
                    }
                });
            } catch (RejectedExecutionException e) {
                semaphore.release();
                throw e;
            }
        }

        public ExecutorService getExecutor(){
            return exec;
        }
    }


}
