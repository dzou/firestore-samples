import com.google.auth.oauth2.GoogleCredentials;
import com.google.firestore.v1.CreateDocumentRequest;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.FirestoreGrpc;
import com.google.firestore.v1.FirestoreGrpc.FirestoreStub;
import com.google.firestore.v1.ListDocumentsRequest;
import com.google.firestore.v1.ListDocumentsResponse;
import com.google.firestore.v1.Value;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.auth.MoreCallCredentials;
import java.io.IOException;
import java.util.HashMap;

public class FirestoreRpcDriver {

  public static void main(String[] args) throws IOException {
    GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
    CallCredentials callCredentials = MoreCallCredentials.from(credentials);

    // Create a channel
    ManagedChannel channel = ManagedChannelBuilder
        .forAddress("firestore.googleapis.com", 443)
        .build();

    FirestoreStub firestore =
        FirestoreGrpc.newStub(channel).withCallCredentials(callCredentials);

    createUser(firestore, "Conor MickGregor", 29);

    readData(firestore);
  }

  public static void createUser(FirestoreStub firestore, String name, int age) {
    HashMap<String, Value> valuesMap = new HashMap<>();
    valuesMap.put("name", Value.newBuilder().setStringValue(name).build());
    valuesMap.put("age", Value.newBuilder().setIntegerValue(age).build());

    CreateDocumentRequest createDocumentRequest =
        CreateDocumentRequest.newBuilder()
            .setParent("projects/cowzow/databases/(default)/documents")
            .setCollectionId("users")
            .setDocumentId(name)
            .setDocument(Document.newBuilder().putAllFields(valuesMap))
            .build();

    ObservableReactiveUtil
        .<Document>unaryCall(obs -> firestore.createDocument(createDocumentRequest, obs))
        .block();
  }

  public static void readData(FirestoreStub firestore) {
    ListDocumentsRequest listDocumentsRequest =
        ListDocumentsRequest.newBuilder()
            .setParent("projects/cowzow/databases/(default)/documents")
            .setCollectionId("users")
            .build();

    ListDocumentsResponse response =
        ObservableReactiveUtil
            .<ListDocumentsResponse>unaryCall(
                obs -> firestore.listDocuments(listDocumentsRequest, obs))
            .block();

    for (Document doc : response.getDocumentsList()) {
      System.out.println("Name: " + doc.getName());
      System.out.println("Keys: " + doc.getFieldsMap());
    }
  }
}
