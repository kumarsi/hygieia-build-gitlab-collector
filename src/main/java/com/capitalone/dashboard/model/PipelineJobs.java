package com.capitalone.dashboard.model;

import com.capitalone.dashboard.collector.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.*;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PipelineJobs {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineJobs.class);
    private List<PipelineJob> jobList = new ArrayList<>();

    public void addJob(JSONObject jsonObject) {
        String stage = (String) jsonObject.get("stage");
        String status = (String) jsonObject.get("status");
        Double durationInDouble = (Double) jsonObject.get("duration");
        Long duration = Math.round(durationInDouble != null ? durationInDouble : 0.0);
        long startedAt = getTime(jsonObject, "started_at");
        long finishedAt = getTime(jsonObject, "finished_at");
        JSONObject commit = (JSONObject) jsonObject.get("commit");
        String commitId = (String) commit.get("id");
        List<String> parentCommitIds = new ArrayList<>();
        Iterator iterator = ((JSONArray) commit.get("parent_ids")).iterator();

        //if pipeline jobs build phase doesn't have time, consume time from pipeline
        JSONObject pipeline = (JSONObject) jsonObject.get("pipeline");
        if (startedAt == 0 && pipeline.containsKey("created_at")) {
            startedAt = getTime(pipeline, "created_at");
            LOG.info(String.format("Getting pipeline start time: %d", startedAt));
        }
        if (finishedAt == 0 && pipeline.containsKey("updated_at")) {
            finishedAt = getTime(pipeline, "updated_at");
            LOG.info(String.format("Getting pipeline end time: %d", finishedAt));
        }
        while (iterator.hasNext()) {
            parentCommitIds.add((String) iterator.next());
        }
        jobList.add(new PipelineJob(stage, startedAt, finishedAt, duration, commitId, parentCommitIds, status));
    }

    public boolean containsIgnoredStages(List<String> ignoredBuildStages) {
        return this.jobList.stream()
                .filter(job -> ignoredBuildStages.contains(job.getStage().toLowerCase()))
                .count() >= 1;
    }

    public long getRelevantJobTime(List<String> buildStages) {
        return this.jobList.stream().filter(job -> buildStages
                .contains(job.getStage().toLowerCase()))
                .map(PipelineJob::getDuration)
                .mapToLong(Long::longValue).sum();
    }

    public long getEarliestStartTime(List<String> buildStages) {
        return this.jobList.stream().filter(job -> buildStages
                .contains(job.getStage().toLowerCase()))
                .map(PipelineJob::getStartedAt)
                .mapToLong(Long::longValue)
                .filter(x -> x != 0)
                .min().orElse(0);
    }

    public long getLastEndTime(List<String> buildStages) {
        return this.jobList.stream().filter(job -> buildStages
                .contains(job.getStage().toLowerCase()))
                .map(PipelineJob::getFinishedAt)
                .mapToLong(Long::longValue).max().orElse(0);
    }

    public BuildStatus getBuildStatus(List<String> buildStages) {
        boolean success = this.jobList.stream().filter(job -> buildStages
                .contains(job.getStage().toLowerCase()))
                .map(PipelineJob::getStatus)
                .allMatch(this::isSuccess);
        return success ? BuildStatus.Success : BuildStatus.Failure;
    }

    private boolean isSuccess(String status) {
        return status.equalsIgnoreCase("success")
                || status.equalsIgnoreCase("manual");
    }

    public Iterable<String> getCommitIds() {
        return Stream.concat(jobList.stream().map(PipelineJob::getCommitId),
                jobList.stream().flatMap(j -> j.getParentCommitIds().stream()))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private long getDateFromString(String dateString) {
        if (dateString == null || dateString.isEmpty())
            return 0L;

        DateTimeFormatter timeFormatter = DateTimeFormatter.ISO_DATE_TIME;
        TemporalAccessor accessor = timeFormatter.parse(dateString);
        return Instant.from(accessor).toEpochMilli();
    }

    private long getTime(JSONObject buildJson, String jsonField) {
        String dateToConsider = getString(buildJson, jsonField);
        if (dateToConsider != null) {
            return getDateFromString(dateToConsider);
        } else {
            return 0L;
        }
    }

    private String getString(JSONObject json, String key) {
        return (String) json.get(key);
    }
}

class PipelineJob {
    private String stage;
    private Long startedAt;
    private Long finishedAt;
    private Long duration;
    private String commitId;
    private List<String> parentCommitIds;
    private String status;

    PipelineJob(String stage, Long startedAt, Long finishedAt, Long duration, String commitId, List<String> parentCommitIds, String status) {
        this.stage = stage;
        this.startedAt = startedAt;
        this.finishedAt = finishedAt;
        this.duration = duration;
        this.commitId = commitId;
        this.parentCommitIds = parentCommitIds;
        this.status = status;
    }

    String getStage() {
        return stage;
    }

    Long getDuration() {
        return duration == null ? 0 : duration;
    }

    Long getStartedAt() {
        return startedAt;
    }

    Long getFinishedAt() {
        return finishedAt;
    }

    String getCommitId() {
        return commitId;
    }

    public List<String> getParentCommitIds() {
        return parentCommitIds;
    }

    public String getStatus() {
        return status;
    }
}
