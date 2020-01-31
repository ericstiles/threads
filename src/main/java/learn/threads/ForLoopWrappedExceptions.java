package learn.threads;

import com.google.common.collect.Lists;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.Thread.sleep;

public class ForLoopWrappedExceptions implements Runnable {

    private static final int CONCURRENCY_FACTOR = 4;
    private static final Duration MAX_STORE_PROCESS_TIME = Duration.of(2, ChronoUnit.SECONDS);
    private static List<Integer> storeIdList = new ArrayList<>();
    {
        IntStream.range(0,100)
                 .forEach(i ->{
                     storeIdList.add(i);
                 });
    }

    public static void main( String[] args )
    {
        System.out.println( "Starting process" );
        new ForLoopWrappedExceptions().run();
    }


    @Override
    public void run() {
        System.out.println(
                "Starting list:"+
                storeIdList);
        ExecutorService executorService = Executors.newFixedThreadPool(CONCURRENCY_FACTOR);
        try {
            for (List<Integer> batch : Lists.partition(storeIdList, CONCURRENCY_FACTOR)) {
                List<Future<List<String>>> futures = new ArrayList<>();
                System.out.println(batch + ": Work that needs to be done");
                for (Integer storeId : batch) {
                    Callable<List<String>> callable =
                            () -> update(List.of(storeId));
                    futures.add(executorService.submit(callable));
                }
                System.out.println(batch + ": Start waiting for each future");
                for (Future<List<String>> future : futures) {
                    List<String> storeResults =
                            future.get(MAX_STORE_PROCESS_TIME.toSeconds(), TimeUnit.SECONDS);
                    storeResults.stream()
                                .forEach(r -> System.out.println(r + ": Post work processing"));
                }
                System.out.println(batch + ": Finished processing assortment for stores");
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            System.out.println("Interrupted waiting for assortment to finish" + ie);
        } catch (TimeoutException te) {
            System.out.println(
                    "Assortment took more than " + MAX_STORE_PROCESS_TIME + " to process a single store:"+ te);
        } catch (ExecutionException ee) {
            System.out.println("updateStoreAssortment encountered exception:" + ee);
        } finally {
//            this.shutdownComplete = true;
            List<Runnable> runnables = executorService.shutdownNow();
            System.out.println("Run complete and shutdown.");
            System.out.println(runnables);
        }
    }

    //UpdateAssortment method
//    List<Integer> causeTimeoutException = List.of(10,50,90);
    List<Integer> causeTimeoutException = List.of();
    List<Integer> causeInterrupt = List.of(11);
    private List<String> update(List<Integer> storeId) throws InterruptedException {
        if (!Collections.disjoint(storeId, causeTimeoutException)) {
            System.out.println("----->Sleep to cause TimeoutException");
            sleep(3100);
        }
        else if (!Collections.disjoint(storeId, causeInterrupt)) {
            throw new InterruptedException("Forced Interrupt");
        }
        else sleep(500);
        return storeId.stream()
                      .peek(i -> System.out.println(i + ": Doing work.  "))
                      .map(i -> i + "-result")
                      .collect(Collectors.toList());
    }

}