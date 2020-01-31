package learn.threads;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

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

@Slf4j
public class ExceptionsInForLoop implements Runnable
{

    private static final int CONCURRENCY_FACTOR = 4;
    private static final Duration MAX_STORE_PROCESS_TIME = Duration.of(2, ChronoUnit.SECONDS);
    private static List<Integer> storeIdList = new ArrayList<>();
    {
        IntStream.rangeClosed(0,100)
                 .forEach(i -> storeIdList.add(i));
    }

    public static void main( String[] args )
    {
        System.out.println( "Starting process" );
        new ExceptionsInForLoop().run();
    }


    @Override
    public void run() {
        System.out.println(
                "Starting list:"+
                        storeIdList);
        ExecutorService executorService = Executors.newFixedThreadPool(CONCURRENCY_FACTOR);
        List<Integer> notProcessed = new ArrayList<>();
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
                    List<String> storeResults = null;
                    try {
                        storeResults = future.get(MAX_STORE_PROCESS_TIME.toSeconds(), TimeUnit.SECONDS);
                        storeResults.stream()
                                    .forEach(r -> System.out.println(r + ": Post work processing"));
                    } catch (InterruptedException e) {
                        System.out.println("----->InterruptedException");
                        notProcessed.addAll(batch);
                    } catch (ExecutionException e) {
                        System.out.println("----->ExecutionException");
                        notProcessed.addAll(batch);
                    } catch (TimeoutException e) {
                        System.out.println("----->TimeoutException");
                        System.out.println(batch + ": adding to not processed list");
                        notProcessed.addAll(batch);
                    }
                    finally {
                        //What to do here?
                    }
                }
                System.out.println(batch + ": Finished looping through batch stores (includes exceptions)");
            }
        } finally {
            List<Runnable> runnables = executorService.shutdownNow();
            System.out.println("Run complete and shutdown.");
        }
        System.out.println("-----------");
        System.out.println("The following integers were not processed and could be retried");
        System.out.println("-->" + notProcessed);
        System.out.println("-->" + notProcessed.stream().collect(Collectors.toSet()));


    }

    //UpdateAssortment method
    List<Integer> causeTimeoutException = List.of(10,50,90);
//    List<Integer> causeTimeoutException = List.of();
    List<Integer> causeInterrupt = List.of(11);
    private List<String> update(List<Integer> storeId) throws InterruptedException {
        if (!Collections.disjoint(storeId, causeTimeoutException)) {
            System.out.println("----->" + storeId + " has element in " + causeTimeoutException);
            System.out.println("----->Sleep to cause TimeoutException");
            sleep(3100);
        }
        else if (!Collections.disjoint(storeId, causeInterrupt)) {
            throw new InterruptedException("Forced Interrupt");
        }
        else sleep(50);
        return storeId.stream()
                      .peek(i -> System.out.println(i + ": Doing work.  "))
                      .map(i -> i + "-result")
                      .collect(Collectors.toList());
    }

}