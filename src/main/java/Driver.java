import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.cloud.firestore.WriteResult;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import reactor.core.publisher.Mono;

public class Driver {
  public static void main(String[] args) throws Exception {
    Firestore firestore = FirestoreOptions.getDefaultInstance().getService();

    // Client Libraries
    addDocument(firestore, "Joe Smith", 26);
    addDocument(firestore, "Johnny O'Connie", 5);
    addDocument(firestore, "Your Uncle", 66);

    readDocuments(firestore, "users");
  }

  public static void addDocument(Firestore firestore, String name, int age)
      throws ExecutionException, InterruptedException {
    DocumentReference docRef = firestore.collection("users").document(name);

    HashMap<String, Object> data = new HashMap<>();
    data.put("name", name);
    data.put("age", age);

    // This thing is an ApiFuture, Can be converted into a Reactor Flux/Mono:
    ApiFuture<WriteResult> future = docRef.set(data);

    // Maybe something like this to convert to a mono:
    Mono.create(sink -> {
      ApiFutures.addCallback(
          future,
          new ApiFutureCallback<WriteResult>() {
            @Override
            public void onFailure(Throwable t) {
              sink.error(t);
            }

            @Override
            public void onSuccess(WriteResult result) {
              sink.success(result);
            }
          },
          null /* Thread pool executor provided here */);
    });
  }

  public static void readDocuments(Firestore firestore, String collectionName)
      throws ExecutionException, InterruptedException {
    // Get and read data at a snapshot
    QuerySnapshot snapshot = firestore.collection("users").get().get();

    for (QueryDocumentSnapshot documentSnapshot : snapshot.getDocuments()) {
      System.out.println(documentSnapshot.getData());
    }
  }

  public static void bigqueryExperiments() throws InterruptedException {
    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(
            "SELECT "
                + "CONCAT('https://stackoverflow.com/questions/', CAST(id as STRING)) as url, "
                + "view_count "
                + "FROM `bigquery-public-data.stackoverflow.posts_questions` "
                + "WHERE tags like '%google-bigquery%' "
                + "ORDER BY favorite_count DESC LIMIT 10")
            // Use standard SQL syntax for queries.
            // See: https://cloud.google.com/bigquery/sql-reference/
            .setUseLegacySql(false)
            .build();

    // Create a job ID so that we can safely retry.
    JobId jobId = JobId.of(UUID.randomUUID().toString());
    Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

    // Wait for the query to complete.
    queryJob = queryJob.waitFor();

    // Check for errors
    if (queryJob == null) {
      throw new RuntimeException("Job no longer exists");
    } else if (queryJob.getStatus().getError() != null) {
      // You can also look at queryJob.getStatus().getExecutionErrors() for all
      // errors, not just the latest one.
      throw new RuntimeException(queryJob.getStatus().getError().toString());
    }
  }
}
