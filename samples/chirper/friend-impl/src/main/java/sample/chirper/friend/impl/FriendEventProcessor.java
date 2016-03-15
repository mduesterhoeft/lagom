package sample.chirper.friend.impl;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.lightbend.lagom.javadsl.persistence.AggregateEventTag;
import com.lightbend.lagom.javadsl.persistence.cassandra.CassandraReadSideProcessor;
import com.lightbend.lagom.javadsl.persistence.cassandra.CassandraSession;

import akka.Done;
import sample.chirper.friend.impl.FriendEvent.FriendAdded;
import sample.chirper.friend.impl.FriendEvent.FriendRemoved;

public class FriendEventProcessor extends CassandraReadSideProcessor<FriendEvent> {

  private PreparedStatement writeFollowers = null; // initialized in prepare
  private PreparedStatement removeFollowers = null; // initialized in prepare
  private PreparedStatement writeOffset = null; // initialized in prepare

  private void setWriteFollowers(PreparedStatement writeFollowers) {
    this.writeFollowers = writeFollowers;
  }

  private void setRemoveFollowers(PreparedStatement removeFollowers) {
    this.removeFollowers = removeFollowers;
  }

  private void setWriteOffset(PreparedStatement writeOffset) {
    this.writeOffset = writeOffset;
  }

  @Override
  public AggregateEventTag<FriendEvent> aggregateTag() {
    return FriendEventTag.INSTANCE;
  }

  @Override
  public CompletionStage<Optional<UUID>> prepare(CassandraSession session) {
    // @formatter:off
    return
      prepareCreateTables(session).thenCompose(a ->
      prepareWriteFollowers(session).thenCompose(b ->
      prepareRemoveFollowers(session).thenCompose(c ->
      prepareWriteOffset(session).thenCompose(d ->
      selectOffset(session)))));
    // @formatter:on
  }

  private CompletionStage<Done> prepareCreateTables(CassandraSession session) {
    // @formatter:off
    return session.executeCreateTable(
        "CREATE TABLE IF NOT EXISTS follower ("
          + "userId text, followedBy text, "
          + "PRIMARY KEY (userId, followedBy))")
      .thenCompose(a -> session.executeCreateTable(
        "CREATE TABLE IF NOT EXISTS friend_offset ("
          + "partition int, offset timeuuid, "
          + "PRIMARY KEY (partition))"));
    // @formatter:on
  }

  private CompletionStage<Done> prepareWriteFollowers(CassandraSession session) {
    return session.prepare("INSERT INTO follower (userId, followedBy) VALUES (?, ?)").thenApply(ps -> {
      setWriteFollowers(ps);
      return Done.getInstance();
    });
  }

  private CompletionStage<Done> prepareRemoveFollowers(CassandraSession session) {
    return session.prepare("DELETE FROM follower where userId = ? and followedBy = ?").thenApply(ps -> {
      setRemoveFollowers(ps);
      return Done.getInstance();
    });
  }

  private CompletionStage<Done> prepareWriteOffset(CassandraSession session) {
    return session.prepare("INSERT INTO friend_offset (partition, offset) VALUES (1, ?)").thenApply(ps -> {
      setWriteOffset(ps);
      return Done.getInstance();
    });
  }

  private CompletionStage<Optional<UUID>> selectOffset(CassandraSession session) {
    return session.selectOne("SELECT offset FROM friend_offset")
        .thenApply(
        optionalRow -> optionalRow.map(r -> r.getUUID("offset")));
  }

  @Override
  public EventHandlers defineEventHandlers(EventHandlersBuilder builder) {
    builder.setEventHandler(FriendAdded.class, this::processFriendChanged);
    builder.setEventHandler(FriendRemoved.class, this::processFriendRemoved);
    return builder.build();
  }

  private CompletionStage<List<BoundStatement>> processFriendChanged(FriendAdded event, UUID offset) {
    BoundStatement bindWriteFollowers = writeFollowers.bind();
    bindWriteFollowers.setString("userId", event.friendId);
    bindWriteFollowers.setString("followedBy", event.userId);
    BoundStatement bindWriteOffset = writeOffset.bind(offset);
    return completedStatements(Arrays.asList(bindWriteFollowers, bindWriteOffset));
  }

  private CompletionStage<List<BoundStatement>> processFriendRemoved(FriendRemoved event, UUID offset) {
    BoundStatement bindRemoveFollowers = removeFollowers.bind();
    bindRemoveFollowers.setString("userId", event.friendId);
    bindRemoveFollowers.setString("followedBy", event.userId);
    BoundStatement bindWriteOffset = writeOffset.bind(offset);
    return completedStatements(Arrays.asList(bindRemoveFollowers, bindWriteOffset));
  }



}
