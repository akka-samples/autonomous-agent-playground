/*
 * Copyright (C) 2021-2026 Lightbend Inc. <https://www.lightbend.com>
 */

package demo.docs;

import akka.javasdk.agent.autonomous.Notification;
import akka.javasdk.agent.task.TaskStatus;
import akka.javasdk.client.ComponentClient;
import akka.stream.Materializer;
import demo.devteam.application.DeveloperTasks;
import demo.docreview.application.ReviewResult;
import demo.docreview.application.ReviewTasks;
import demo.helloworld.application.QuestionAnswerer;
import demo.pipeline.application.ReportAgent;
import demo.research.application.ResearchBrief;
import demo.research.application.ResearchTasks;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Code snippets for the autonomous-agent documentation pages. Each method body is included in the
 * docs by tag. The class is never executed; it is compiled so the examples stay in step with the
 * SDK and the sample code they reference.
 */
class ClientApiSample {

  private static final Logger log = LoggerFactory.getLogger(ClientApiSample.class);

  private final ComponentClient componentClient;
  private final Materializer materializer;

  ClientApiSample(ComponentClient componentClient, Materializer materializer) {
    this.componentClient = componentClient;
    this.materializer = materializer;
  }

  void terminate(String agentInstanceId) {
    componentClient.forAutonomousAgent(ReportAgent.class, agentInstanceId).terminate();
  }

  void suspendAndResume(String agentInstanceId) {
    // Suspend the agent
    componentClient.forAutonomousAgent(ReportAgent.class, agentInstanceId).suspend();

    // Resume the agent
    componentClient.forAutonomousAgent(ReportAgent.class, agentInstanceId).resume();
  }

  void agentState(String agentInstanceId) {
    var state = componentClient
      .forAutonomousAgent(ReportAgent.class, agentInstanceId)
      .getState();

    state.phase(); // "idle", "advance", "model", "tools", or "stopped"
    state.suspended(); // whether the agent is suspended
    state.instructions(); // the agent's current instructions
    state.totalTokenUsage(); // cumulative token usage (inputTokens, outputTokens)
    state.currentTask(); // Optional<TaskKey> with the task currently being worked on
    state.pendingTaskIds(); // List<String> with ids of tasks queued but not yet started
  }

  void querySnapshot(String taskId) {
    var snapshot = componentClient.forTask(taskId).get(ResearchTasks.BRIEF);
    if (snapshot.status() == TaskStatus.COMPLETED) {
      ResearchBrief brief = snapshot.result().orElseThrow();
      log.info("{}: {}", brief.title(), brief.keyFindings());
    }
  }

  void awaitResult(String taskId) {
    ResearchBrief brief = componentClient.forTask(taskId).result(ResearchTasks.BRIEF);
    log.info("Research brief: {}", brief.title());
  }

  void subscribeNotifications(String agentInstanceId) {
    componentClient
      .forAutonomousAgent(QuestionAnswerer.class, agentInstanceId)
      .notificationStream()
      .runForeach(System.out::println, materializer);
  }

  void notificationFamilies(String agentInstanceId) {
    var agentClient = componentClient.forAutonomousAgent(
      QuestionAnswerer.class,
      agentInstanceId
    );
    agentClient
      .notificationStream()
      .runForeach(
        n -> {
          switch (n) {
            case Notification.LifecycleNotification lifecycle -> renderLifecycle(lifecycle);
            case Notification.TaskNotification task -> renderTask(task);
            case Notification.TeamNotification team -> renderTeam(team);
            default -> {} // ignore
          }
        },
        materializer
      );
  }

  void asyncCalls(String agentInstanceId) {
    var stateF = componentClient
      .forAutonomousAgent(ReportAgent.class, agentInstanceId)
      .getStateAsync();

    stateF.thenAccept(state -> log.info("Phase: {}", state.phase()));
  }

  void resolveTemplate() {
    var task = DeveloperTasks.IMPLEMENT.params(
      Map.of(
        "feature",
        "rate limiter",
        "requirements",
        "10 requests per second per user, with 1-minute window"
      )
    );
    log.info("Resolved task: {}", task);
  }

  void snapshotFields(String taskId) {
    var snapshot = componentClient.forTask(taskId).get(ReviewTasks.REVIEW);
    // PENDING, ASSIGNED, IN_PROGRESS, RESULT_REJECTED, COMPLETED, FAILED, or CANCELLED
    TaskStatus status = snapshot.status();
    // present if completed
    Optional<ReviewResult> result = snapshot.result();
    // present if failed
    Optional<String> reason = snapshot.failureReason();
    log.info("{} {} {}", status, result, reason);
  }

  private void renderLifecycle(Notification.LifecycleNotification notification) {}

  private void renderTask(Notification.TaskNotification notification) {}

  private void renderTeam(Notification.TeamNotification notification) {}
}
