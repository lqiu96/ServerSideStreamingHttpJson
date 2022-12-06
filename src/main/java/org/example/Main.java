package org.example;

import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.ServerStreamingCallable;
import com.google.cloud.spanner.v1.SpannerClient;
import com.google.cloud.spanner.v1.SpannerSettings;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.*;

import java.io.IOException;
import java.util.List;

public class Main {

    public static final String PROJECT_ID = "lawrenceqiu-tf-25559";
    public static final String INSTANCE_ID = "server-streaming-test";
    public static final String DATABASE_ID = "test-spanner";

    public static void main(String[] args) throws IOException {
        // Populate the Database
//        SpannerOptions options = SpannerOptions.newBuilder().build();
//        try (Spanner spanner = options.getService()) {
//            DatabaseId db = DatabaseId.of(PROJECT_ID, INSTANCE_ID, DATABASE_ID);
//            DatabaseClient dbClient = spanner.getDatabaseClient(db);
//            populateData(dbClient);
//        } catch (Exception e) {
//            throw e;
//        }

        KeyRange keyRange = KeyRange.newBuilder()
                .setStartOpen(ListValue.newBuilder().addValues(Value.newBuilder().setStringValue("0")))
                .setEndClosed(ListValue.newBuilder().addValues(Value.newBuilder().setStringValue("100000"))).build();

        SpannerSettings spannerSettings = SpannerSettings.newBuilder()
                .setTransportChannelProvider(SpannerSettings.defaultHttpJsonTransportProviderBuilder().build()).build();
        try (SpannerClient spannerClient = SpannerClient.create(spannerSettings)) {
            DatabaseName database = DatabaseName.of(PROJECT_ID, INSTANCE_ID, DATABASE_ID);
            com.google.spanner.v1.Session sessionName = spannerClient.createSession(database);
            ServerStreamingCallable<ReadRequest, PartialResultSet> serverStreamingCallable = spannerClient.streamingReadCallable();
            ServerStream<PartialResultSet> serverStream = serverStreamingCallable.call(
                    ReadRequest.newBuilder()
                            .setSession(sessionName.getName())
                            .setTransaction(TransactionSelector.newBuilder().build())
                            .setTable("SpannerTestTable")
                            .setKeySet(KeySet.newBuilder().addRanges(keyRange).build())
                            .addAllColumns(List.of("RowId"))
                            .build());
            // TODO: Need to confirm this. Does it aggregate all the results before returning?
            for (PartialResultSet partialResultSet : serverStream) {
                System.out.println(partialResultSet);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println("done");
    }

//    private static void populateData(DatabaseClient client) {
//        List<Mutation> mutations = new ArrayList<>();
//        for (int i = 0; i < 10000; i++) {
//            mutations.add(
//                    Mutation.newInsertBuilder("SpannerTestTable")
//                            .set("RowId")
//                            .to(i)
//                            .build());
//        }
//        client.write(mutations);
//    }
}