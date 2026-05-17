import java.util.concurrent.*;
import java.util.*;

// Статистика довжин слів (результат обчислень)
class Statistics {

    private long   count;   // кількість слів
    private long   sum;     // сума довжин
    private long   sum2;    // сума квадратів довжин (для дисперсії)
    private int    min;     // мінімальна довжина
    private int    max;     // максимальна довжина

    public Statistics(long count, long sum, long sum2, int min, int max) {
        this.count = count;
        this.sum   = sum;
        this.sum2  = sum2;
        this.min   = min;
        this.max   = max;
    }

    // Злиття двох часткових результатів
    public static Statistics merge(Statistics a, Statistics b) {
        return new Statistics(
                a.count + b.count,
                a.sum   + b.sum,
                a.sum2  + b.sum2,
                Math.min(a.min, b.min),
                Math.max(a.max, b.max)
        );
    }

    public double mean()     { return (double) sum / count; }
    public double variance() { return (double) sum2 / count - mean() * mean(); }
    public double stddev()   { return Math.sqrt(variance()); }
    public long   getCount() { return count; }
    public int    getMin()   { return min; }
    public int    getMax()   { return max; }

    public void print() {
        System.out.println("Кількість слів : " + count);
        System.out.printf ("Мінімум        : %d символів%n",   min);
        System.out.printf ("Максимум       : %d символів%n",   max);
        System.out.printf ("Математичне очікування (середня): %.4f%n", mean());
        System.out.printf ("Дисперсія                       : %.4f%n", variance());
        System.out.printf ("Середньоквадратичне відхилення  : %.4f%n", stddev());
    }
}

// ForkJoin-задача: аналіз підмасиву слів
class WordLengthTask extends RecursiveTask<Statistics> {

    private static final int THRESHOLD = 500; // поріг розбиття

    private final String[] words;
    private final int from;
    private final int to;

    public WordLengthTask(String[] words, int from, int to) {
        this.words = words;
        this.from  = from;
        this.to    = to;
    }

    @Override
    protected Statistics compute() {
        int length = to - from;

        // Якщо підмасив достатньо малий — обчислюємо безпосередньо
        if (length <= THRESHOLD) {
            return computeDirectly();
        }

        // Інакше — ділимо навпіл і запускаємо дві підзадачі
        int mid = from + length / 2;

        WordLengthTask leftTask  = new WordLengthTask(words, from, mid);
        WordLengthTask rightTask = new WordLengthTask(words, mid, to);

        leftTask.fork();                          // асинхронний запуск лівої
        Statistics rightResult = rightTask.invoke(); // синхронний запуск правої
        Statistics leftResult  = leftTask.join();    // очікуємо ліву

        return Statistics.merge(leftResult, rightResult);
    }

    private Statistics computeDirectly() {
        long count = 0, sum = 0, sum2 = 0;
        int  min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;

        for (int i = from; i < to; i++) {
            int len = words[i].length();
            if (len == 0) continue;
            count++;
            sum  += len;
            sum2 += (long) len * len;
            if (len < min) min = len;
            if (len > max) max = len;
        }

        if (count == 0) return new Statistics(0, 0, 0, 0, 0);
        return new Statistics(count, sum, sum2, min, max);
    }
}

public class TextAnalysis {

    private static final String SAMPLE_TEXT =
            "For decades Canada was seen as a global laggard in defence funding and just " +
                    "two years ago recruitment was so dire that a former defence minister warned " +
                    "the armed forces were in a death spiral. Now the Canadian army is growing at " +
                    "a pace not seen in decades reaching its highest number of recruits in 30 years " +
                    "and potentially reversing the chronic personnel shortage that has plagued the " +
                    "country's military. The boost over the last two years comes as the world grapples " +
                    "with major armed conflicts and geopolitical uncertainty and as Canada commits " +
                    "billions in new military funding after years of falling short of its Nato obligations. " +
                    "It also coincides with an uncharacteristic rise in nationalism that has emerged since " +
                    "US President Donald Trump referred to Canada as the 51st state remarks that many " +
                    "viewed as a threat to the country's sovereignty from its closest neighbour. " +
                    "When people see that the world is not as safe that their country might be at risk " +
                    "we tend to see people join the military. Global conflicts are not the only factor " +
                    "driving the increase. Canada's high youth unemployment rate which hovered at nearly " +
                    "14 percent in March as well as the promise of job security and higher wages after " +
                    "Prime Minister Mark Carney announced the largest pay increase for military personnel " +
                    "in a generation are also a factor. ";

    public static void main(String[] args) throws Exception {

        // Генеруємо великий масив слів повторенням тексту
        String[] baseWords = SAMPLE_TEXT.trim().split("\\s+");
        int repeat = 200; // ~26000 слів
        String[] words = new String[baseWords.length * repeat];
        for (int i = 0; i < repeat; i++) {
            System.arraycopy(baseWords, 0, words, i * baseWords.length, baseWords.length);
        }

        System.out.println("Текст завантажено. Слів для аналізу: " + words.length);
        System.out.println();

        // ForkJoin аналіз
        ForkJoinPool pool = new ForkJoinPool();
        WordLengthTask task = new WordLengthTask(words, 0, words.length);

        long startTime = System.currentTimeMillis();
        Statistics stats = pool.invoke(task);
        long forkJoinTime = System.currentTimeMillis() - startTime;

        pool.shutdown();

        System.out.println("=== Характеристики випадкової величини \"довжина слова\" ===");
        stats.print();
        System.out.printf("%nЧас виконання: %d мс%n", forkJoinTime);
    }
}
