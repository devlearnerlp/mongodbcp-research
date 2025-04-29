@RestController
@RequestMapping("/api/mongo")
public class MongoCopyController {

    private final Map<String, CopyStatus> statusMap = new ConcurrentHashMap<>();

    @PostMapping("/copy")
    public ResponseEntity<String> copyMongoData(@RequestBody MongoCopyRequest request) {
        String jobId = UUID.randomUUID().toString();
        
        new Thread(() -> {
            try (MongoClient sourceClient = MongoClients.create(request.sourceUri);
                 MongoClient targetClient = MongoClients.create(request.targetUri)) {

                MongoDatabase sourceDb = sourceClient.getDatabase(request.sourceDb);
                MongoDatabase targetDb = targetClient.getDatabase(request.targetDb);

                if (request.collection != null && !request.collection.isEmpty()) {
                    copyCollection(sourceDb, targetDb, request.collection, jobId);
                } else {
                    for (String collectionName : sourceDb.listCollectionNames()) {
                        copyCollection(sourceDb, targetDb, collectionName, jobId);
                    }
                }
                statusMap.get(jobId).setStatus("SUCCESS");
            } catch (Exception e) {
                CopyStatus failure = new CopyStatus();
                failure.setStatus("FAILED");
                failure.setErrorMessage(e.getMessage());
                statusMap.put(jobId, failure);
                e.printStackTrace();
            }
        }).start();
        
        return ResponseEntity.ok(jobId); // Return jobId
    }

    @PostMapping("/pause/{jobId}")
    public ResponseEntity<String> pauseJob(@PathVariable String jobId) {
        CopyStatus status = statusMap.get(jobId);
        if (status != null && status.getStatus().equals("IN_PROGRESS")) {
            status.setPauseRequested(true);
            return ResponseEntity.ok("Pause requested");
        }
        return ResponseEntity.status(400).body("Cannot pause - Job not in progress");
    }

    @PostMapping("/resume/{jobId}")
    public ResponseEntity<String> resumeJob(@PathVariable String jobId) {
        CopyStatus status = statusMap.get(jobId);
        if (status != null && status.getStatus().equals("PAUSED")) {
            status.setPauseRequested(false);
            status.setStatus("IN_PROGRESS");
            return ResponseEntity.ok("Resume requested");
        }
        return ResponseEntity.status(400).body("Cannot resume - Job not paused");
    }

    @GetMapping("/status/{jobId}")
    public ResponseEntity<CopyStatus> getStatus(@PathVariable String jobId) {
        CopyStatus status = statusMap.get(jobId);
        if (status == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(status);
    }

    private void copyCollection(MongoDatabase sourceDb, MongoDatabase targetDb, String collectionName, String jobId) {
        MongoCollection<Document> source = sourceDb.getCollection(collectionName);
        MongoCollection<Document> target = targetDb.getCollection(collectionName);
        target.drop(); // Remove if you don't want to wipe target collection first

        long totalDocs = source.countDocuments();
        CopyStatus status = new CopyStatus();
        status.setCollection(collectionName);
        status.setTotalDocs(totalDocs);
        status.setCopiedDocs(0);
        status.setStatus("IN_PROGRESS");
        statusMap.put(jobId, status);

        try (MongoCursor<Document> cursor = source.find().iterator()) {
            List<Document> batch = new ArrayList<>();
            while (cursor.hasNext()) {
                // Check if pause requested
                while (status.isPauseRequested()) {
                    status.setStatus("PAUSED");
                    Thread.sleep(500); // Sleep while paused
                }
                status.setStatus("IN_PROGRESS");

                batch.add(cursor.next());
                if (batch.size() == 1000) {
                    target.insertMany(batch);
                    batch.clear();
                    status.setCopiedDocs(status.getCopiedDocs() + 1000);
                }
            }
            if (!batch.isEmpty()) {
                target.insertMany(batch);
                status.setCopiedDocs(status.getCopiedDocs() + batch.size());
            }
        } catch (Exception e) {
            status.setStatus("FAILED");
            status.setErrorMessage(e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
