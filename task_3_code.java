import java.util.concurrent.*;

// ForkJoin: паралельне обчислення суми рахунків
class SumTask extends RecursiveTask<Long> {

    private static final int THRESHOLD = 10;
    private final int[] accounts;
    private final int from;
    private final int to;

    SumTask(int[] accounts, int from, int to) {
        this.accounts = accounts;
        this.from = from;
        this.to   = to;
    }

    @Override
    protected Long compute() {
        if (to - from <= THRESHOLD) {
            // Базовий випадок — рахуємо суму безпосередньо
            long sum = 0;
            for (int i = from; i < to; i++) sum += accounts[i];
            return sum;
        }
        // Ділимо масив навпіл і запускаємо дві підзадачі
        int mid = from + (to - from) / 2;
        SumTask left  = new SumTask(accounts, from, mid);
        SumTask right = new SumTask(accounts, mid,  to);
        left.fork();
        long rightResult = right.invoke();
        long leftResult  = left.join();
        return leftResult + rightResult;
    }
}

public class BankForkJoin {

    private static final int NACCOUNTS       = 1_000_000;
    private static final int INITIAL_BALANCE = 10000;
    private static final int RUNS            = 10;

    // Однопотокове обчислення суми — базовий час T1
    static long singleThreadSum(int[] accounts) {
        long sum = 0;
        for (int a : accounts) sum += a;
        return sum;
    }

    public static void main(String[] args) throws Exception {

        // Ініціалізуємо масив рахунків
        int[] accounts = new int[NACCOUNTS];
        for (int i = 0; i < NACCOUNTS; i++) accounts[i] = INITIAL_BALANCE;
        long expected = (long) NACCOUNTS * INITIAL_BALANCE;

        System.out.println("Рахунків : " + NACCOUNTS);
        System.out.println("Очікувана сума: " + expected);
        System.out.println("Запусків для усереднення: " + RUNS);
        System.out.println();

        int[] threadCounts = {1, 2, 4, 8};

        // Однопотоковий час T1
        long t1 = Long.MAX_VALUE;
        for (int r = 0; r < RUNS; r++) {
            long t = System.nanoTime();
            long sum = singleThreadSum(accounts);
            t1 = Math.min(t1, System.nanoTime() - t);
            assert sum == expected;
        }

        System.out.printf("%-10s %-16s %-16s %-12s %-10s%n",
                "Потоків", "T1 (нс)", "Tp (нс)", "S = T1/Tp", "E = S/p");
        System.out.println("-".repeat(66));

        for (int p : threadCounts) {
            long tp = Long.MAX_VALUE;
            for (int r = 0; r < RUNS; r++) {
                ForkJoinPool pool = new ForkJoinPool(p);
                long t = System.nanoTime();
                long sum = pool.invoke(new SumTask(accounts, 0, accounts.length));
                tp = Math.min(tp, System.nanoTime() - t);
                pool.shutdown();
                assert sum == expected;
            }
            double speedup    = (double) t1 / tp;
            double efficiency = speedup / p;
            System.out.printf("%-10d %-16d %-16d %-12.3f %-10.3f%n",
                    p, t1, tp, speedup, efficiency);
        }

        System.out.println();
        System.out.println("Результат ForkJoin: " +
                new ForkJoinPool().invoke(new SumTask(accounts, 0, accounts.length))
                + " (коректність: " + (new ForkJoinPool().invoke(
                new SumTask(accounts, 0, accounts.length)) == expected
                ? "OK" : "FAIL") + ")");
    }
}
