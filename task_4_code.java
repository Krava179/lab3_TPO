import java.util.*;
import java.util.concurrent.*;

// ForkJoin: збір унікальних слів з підмасиву
class CollectWordsTask extends RecursiveTask<Set<String>> {

    private static final int THRESHOLD = 300;
    private final String[] words;
    private final int from;
    private final int to;

    CollectWordsTask(String[] words, int from, int to) {
        this.words = words;
        this.from  = from;
        this.to    = to;
    }

    @Override
    protected Set<String> compute() {
        if (to - from <= THRESHOLD) {
            Set<String> result = new HashSet<>();
            for (int i = from; i < to; i++) {
                String w = words[i].toLowerCase().replaceAll("[^a-zA-Zа-яА-ЯіІїЇєЄ]", "");
                if (!w.isEmpty()) result.add(w);
            }
            return result;
        }
        int mid = from + (to - from) / 2;
        CollectWordsTask left  = new CollectWordsTask(words, from, mid);
        CollectWordsTask right = new CollectWordsTask(words, mid,  to);
        left.fork();
        Set<String> rightSet = right.invoke();
        Set<String> leftSet  = left.join();
        leftSet.addAll(rightSet); // зливаємо два набори
        return leftSet;
    }
}

public class CommonWords {

    static final String TEXT_1 =
            "For decades Canada was seen as a global laggard in defence funding and just " +
                    "two years ago recruitment was so dire that a former defence minister warned " +
                    "the armed forces were in a death spiral. Now the Canadian army is growing at " +
                    "a pace not seen in decades reaching its highest number of recruits in 30 years " +
                    "and potentially reversing the chronic personnel shortage that has plagued the " +
                    "country's military. The boost over the last two years comes as the world grapples " +
                    "with major armed conflicts and geopolitical uncertainty and as Canada commits " +
                    "billions in new military funding after years of falling short of its Nato obligations. ";

    static final String TEXT_2 =
            "Ukraine has been at the center of a major armed conflict since 2022 when Russia " +
                    "launched a full scale invasion. The country has received billions in military " +
                    "funding and weapons from Nato allies including Canada and other world powers. " +
                    "Recruitment into the Ukrainian armed forces has grown significantly as the war " +
                    "continues with hundreds of thousands of personnel serving in various military roles. " +
                    "The conflict has created uncertainty across Europe and raised questions about " +
                    "defence obligations among alliance members. Many countries have committed to " +
                    "increasing their military spending in response to the growing threat. ";

    static String[] toWords(String text) {
        return text.trim().split("\\s+");
    }

    public static void main(String[] args) throws Exception {
        String[] words1 = toWords(TEXT_1);
        String[] words2 = toWords(TEXT_2);

        System.out.println("Документ 1: " + words1.length + " слів");
        System.out.println("Документ 2: " + words2.length + " слів");
        System.out.println();

        ForkJoinPool pool = new ForkJoinPool();

        // Паралельно збираємо унікальні слова з обох документів
        CollectWordsTask task1 = new CollectWordsTask(words1, 0, words1.length);
        CollectWordsTask task2 = new CollectWordsTask(words2, 0, words2.length);

        long start = System.nanoTime();

        task1.fork(); // запускаємо першу задачу асинхронно
        Set<String> set2 = pool.invoke(task2); // виконуємо другу
        Set<String> set1 = task1.join();        // чекаємо першу

        // Знаходимо перетин — спільні слова
        Set<String> common = new HashSet<>(set1);
        common.retainAll(set2);

        long elapsed = System.nanoTime() - start;

        pool.shutdown();

        System.out.println("Унікальних слів у документі 1: " + set1.size());
        System.out.println("Унікальних слів у документі 2: " + set2.size());
        System.out.println("Спільних слів: " + common.size());
        System.out.println();

        // Сортуємо
        List<String> sorted = new ArrayList<>(common);
        Collections.sort(sorted);
        System.out.println("Список спільних слів:");
        System.out.println(sorted);
        System.out.printf("%nЧас виконання: %d нс%n", elapsed);
    }
}
