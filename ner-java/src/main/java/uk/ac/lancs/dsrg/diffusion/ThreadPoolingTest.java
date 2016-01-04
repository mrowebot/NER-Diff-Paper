package uk.ac.lancs.dsrg.diffusion;

import java.util.*;
import java.util.concurrent.*;

/**
 * Author: Matthew Rowe
 * Email: m.rowe@lancaster.ac.uk
 * Date / Time : 24/11/15 / 14:34
 */
public class ThreadPoolingTest {

    public static void main(final String[] argv) {

        // generate the pool thread
        ExecutorService service = Executors.newFixedThreadPool(10);
        Collection<Foo> tasks = new ArrayList<Foo>();

        // submit two test tasks
        for (int i = 0; i < 20; i++) {
            for (int j = 0; j < 20; j++) {
                tasks.add(new Foo("u" + i, "v"+j));
            }
        }

        try {
            List<Future<HashMap<String,Object>>> results = service.invokeAll(tasks);
            for (Future<HashMap<String, Object>> result : results) {
                HashMap<String, Object> resultMap = result.get();
                double prob_uv = (Double)resultMap.get("prob");
                String uLocal = (String)resultMap.get("u");
                String vLocal = (String)resultMap.get("v");

                System.out.println(uLocal + " -> " + vLocal + ". P=" + prob_uv);
            }


        } catch(final InterruptedException ex) {
            ex.printStackTrace();
        } catch(final Exception ex) {
            ex.printStackTrace();
        }

        service.shutdownNow();
    }
}

class Foo implements Callable<HashMap<String, Object>> {

    int probSetting;
    int modelSetting;
    String u;
    String v;
    long date;
    boolean inResultsTable;

    Foo(String u, String v) {
        this.u = u;
        this.v = v;
    }

    Foo(int probSetting, int modelSetting, String u, String v, long date, boolean inResultsTable) {
        this.probSetting = probSetting;
        this.modelSetting = modelSetting;
        this.u = u;
        this.v = v;
        this.date = date;
        this.inResultsTable = inResultsTable;
    }

    public HashMap<String, Object> call() {
//        try {
//            // sleep for 10 seconds
////            Thread.sleep(10 * 1000);
//        } catch(final InterruptedException ex) {
//            ex.printStackTrace();
//        }

        Random rand = new Random();
        double prob_uv = rand.nextDouble();

        // return map
        HashMap<String, Object> result = new HashMap<String, Object>();
        result.put("u", this.u);
        result.put("v", this.v);
        result.put("prob", prob_uv);

//        return rand.nextDouble();
        return result;
    }
}
