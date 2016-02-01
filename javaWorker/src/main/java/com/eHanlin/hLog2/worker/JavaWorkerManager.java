package com.eHanlin.hLog2.worker;

public class JavaWorkerManager implements Runnable {

    protected boolean isShutdownStarted = false;
    protected boolean isShutdownEnded = false;

    protected Runnable worker = null;

    public JavaWorkerManager(Runnable worker) {
        this.worker = worker;
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                System.out.println("JVM start shutdown");
                shutdown();
                while(!isShutdownEnded){
                    try {
                        sleep(100L);
                    } catch (InterruptedException e) {

                    }
                }
                System.out.println("JVM finish shutdown");
            }
        });
    }

    @Override
    public void run() {
        System.out.println("Worker start");
        while(!isShutdownStarted){
            try {
                worker.run();
            } catch (Exception e) {
                System.out.println(e.toString());
            }
        }
        System.out.println("Worker stop");
        isShutdownEnded = true;
        System.out.println("Worker finish shutdown");
    }

    public void shutdown() {
        System.out.println("Worker start shutdown");
        isShutdownStarted = true;
    }

}
