import java.util.*;
import java.util.concurrent.*;

class Document {
    final String title;
    final String text;

    Document(String title, String text) {
        this.title = title;
        this.text  = text;
    }
}

// Результат пошуку: документ + кількість знайдених ключових слів
class SearchResult {
    final String title;
    final int    matches;
    final Set<String> foundKeywords;

    SearchResult(String title, int matches, Set<String> foundKeywords) {
        this.title        = title;
        this.matches      = matches;
        this.foundKeywords = foundKeywords;
    }
}

// ForkJoin: пошук ключових слів у підмасиві документів
class SearchTask extends RecursiveTask<List<SearchResult>> {

    private static final int THRESHOLD = 2;
    private final List<Document> docs;
    private final Set<String> keywords;
    private final int from;
    private final int to;

    SearchTask(List<Document> docs, Set<String> keywords, int from, int to) {
        this.docs     = docs;
        this.keywords = keywords;
        this.from     = from;
        this.to       = to;
    }

    @Override
    protected List<SearchResult> compute() {
        if (to - from <= THRESHOLD) {
            return searchDirectly();
        }
        int mid = from + (to - from) / 2;
        SearchTask left  = new SearchTask(docs, keywords, from, mid);
        SearchTask right = new SearchTask(docs, keywords, mid,  to);
        left.fork();
        List<SearchResult> rightResult = right.invoke();
        List<SearchResult> leftResult  = left.join();
        // зливаємо результати
        List<SearchResult> merged = new ArrayList<>(leftResult);
        merged.addAll(rightResult);
        return merged;
    }

    private List<SearchResult> searchDirectly() {
        List<SearchResult> results = new ArrayList<>();
        for (int i = from; i < to; i++) {
            Document doc = docs.get(i);
            Set<String> words = new HashSet<>(
                    Arrays.asList(doc.text.toLowerCase().split("[\\s\\p{Punct}]+")));
            Set<String> found = new HashSet<>();
            for (String kw : keywords) {
                if (words.contains(kw.toLowerCase())) found.add(kw);
            }
            if (!found.isEmpty()) {
                results.add(new SearchResult(doc.title, found.size(), found));
            }
        }
        return results;
    }
}

public class DocumentSearch {

    public static void main(String[] args) throws Exception {

        // Набір документів
        List<Document> docs = Arrays.asList(
                new Document("Machine Learning Basics",
                        "Machine learning is a subset of artificial intelligence that enables " +
                                "systems to learn from data and improve their performance without explicit " +
                                "programming. Algorithms such as neural networks and decision trees are " +
                                "widely used in classification and regression tasks."),

                new Document("History of Ancient Rome",
                        "Ancient Rome was a civilization that grew from a small agricultural " +
                                "community on the Italian Peninsula in the 9th century BC. The Roman " +
                                "Empire expanded across Europe, North Africa and the Middle East."),

                new Document("Cloud Computing Architecture",
                        "Cloud computing provides on-demand access to computing resources such as " +
                                "servers, storage and databases over the internet. Major providers include " +
                                "platforms built on virtualization, microservices and distributed systems."),

                new Document("Cooking Italian Pasta",
                        "Italian pasta is one of the most beloved dishes in world cuisine. " +
                                "Traditional recipes use semolina flour, eggs and water. Popular varieties " +
                                "include spaghetti, fettuccine and penne with various sauces."),

                new Document("Cybersecurity and Network Protection",
                        "Cybersecurity involves protecting computer systems and networks from " +
                                "digital attacks. Key practices include encryption, firewalls, intrusion " +
                                "detection and secure software development to prevent data breaches."),

                new Document("Database Management Systems",
                        "A database management system provides tools for storing, retrieving and " +
                                "managing structured data. SQL and NoSQL databases differ in their data " +
                                "models, with relational databases using tables and keys for data integrity."),

                new Document("Wildlife Conservation in Africa",
                        "Africa is home to a vast diversity of wildlife including elephants, lions " +
                                "and giraffes. Conservation efforts focus on protecting natural habitats " +
                                "and reducing poaching through international cooperation and funding."),

                new Document("Software Development Methodologies",
                        "Modern software development relies on agile methodologies, version control " +
                                "systems and continuous integration pipelines. Teams use tools like Git, " +
                                "Docker and Kubernetes to manage code and deploy applications efficiently."),

                new Document("Quantum Computing Fundamentals",
                        "Quantum computing uses quantum bits or qubits to perform computations that " +
                                "classical computers cannot efficiently solve. Applications include " +
                                "cryptography, optimization and simulation of complex systems."),

                new Document("Renewable Energy Sources",
                        "Renewable energy includes solar, wind and hydroelectric power. These " +
                                "sources reduce dependence on fossil fuels and lower carbon emissions. " +
                                "Investment in renewable infrastructure has grown significantly worldwide.")
        );

        // Ключові слова з області ІТ
        Set<String> keywords = new LinkedHashSet<>(Arrays.asList(
                "algorithm", "data", "network", "software", "computing",
                "database", "encryption", "programming", "systems", "neural"
        ));

        System.out.println("Документів для пошуку : " + docs.size());
        System.out.println("Ключові слова ІТ: " + keywords);
        System.out.println();

        // ForkJoin пошук
        ForkJoinPool pool = new ForkJoinPool();
        SearchTask task = new SearchTask(docs, keywords, 0, docs.size());

        long start = System.nanoTime();
        List<SearchResult> results = pool.invoke(task);
        long elapsed = System.nanoTime() - start;

        pool.shutdown();

        // Сортуємо за кількістю збігів (найрелевантніші — першими)
        results.sort((a, b) -> b.matches - a.matches);

        System.out.println("Знайдено документів: " + results.size());
        System.out.println();
        System.out.printf("%-5s %-45s %-8s %s%n", "№", "Документ", "Збігів", "Знайдені ключові слова");
        System.out.println("-".repeat(90));
        int rank = 1;
        for (SearchResult r : results) {
            List<String> sorted = new ArrayList<>(r.foundKeywords);
            Collections.sort(sorted);
            System.out.printf("%-5d %-45s %-8d %s%n", rank++, r.title, r.matches, sorted);
        }

        System.out.printf("%nЧас виконання: %d нс%n", elapsed);
    }
}
