package com.rishi.redis.pubsub;

import com.rishi.redis.protocol.RespValue;
import com.rishi.redis.server.ClientSession;
import com.rishi.redis.store.GlobPattern;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TD-10 / FR-8 — channel and pattern routing.
 *
 * <p>SUBSCRIBE registers the connection under a channel name; PUBLISH is then an O(1)
 * hash lookup followed by a fan-out onto each subscriber's non-blocking write buffer.
 * The publisher never waits for a slow subscriber — it hands the frame to the sink and
 * returns — which is what keeps one stalled client from backing up the whole channel.
 *
 * <p>Pattern subscriptions live in a second map and are matched with the same glob
 * engine KEYS uses. Pattern matching is linear in the number of distinct patterns
 * registered, not in the number of subscribers.
 */
public final class PubSubRegistry {

    private static final String MESSAGE = "message";
    private static final String PMESSAGE = "pmessage";

    private final ConcurrentHashMap<String, Set<ClientSession>> channels = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<ClientSession>> patterns = new ConcurrentHashMap<>();

    /** @return the session's total subscription count after subscribing */
    public int subscribe(ClientSession session, String channel) {
        channels.computeIfAbsent(channel, c -> ConcurrentHashMap.newKeySet()).add(session);
        session.channels().add(channel);
        return session.subscriptionCount();
    }

    public int unsubscribe(ClientSession session, String channel) {
        session.channels().remove(channel);
        Set<ClientSession> subscribers = channels.get(channel);
        if (subscribers != null) {
            subscribers.remove(session);
            if (subscribers.isEmpty()) {
                channels.remove(channel, subscribers);
            }
        }
        return session.subscriptionCount();
    }

    public int psubscribe(ClientSession session, String pattern) {
        patterns.computeIfAbsent(pattern, p -> ConcurrentHashMap.newKeySet()).add(session);
        session.patterns().add(pattern);
        return session.subscriptionCount();
    }

    public int punsubscribe(ClientSession session, String pattern) {
        session.patterns().remove(pattern);
        Set<ClientSession> subscribers = patterns.get(pattern);
        if (subscribers != null) {
            subscribers.remove(session);
            if (subscribers.isEmpty()) {
                patterns.remove(pattern, subscribers);
            }
        }
        return session.subscriptionCount();
    }

    /** @return how many clients received the message (channel + pattern subscribers) */
    public long publish(String channel, String message) {
        long delivered = 0;

        Set<ClientSession> direct = channels.get(channel);
        if (direct != null) {
            RespValue frame = RespValue.array(List.of(
                    RespValue.bulk(MESSAGE),
                    RespValue.bulk(channel),
                    RespValue.bulk(message)));
            for (ClientSession subscriber : direct) {
                if (deliver(subscriber, frame)) {
                    delivered++;
                } else {
                    direct.remove(subscriber);
                }
            }
        }

        for (Map.Entry<String, Set<ClientSession>> entry : patterns.entrySet()) {
            if (!GlobPattern.matches(entry.getKey(), channel)) {
                continue;
            }
            RespValue frame = RespValue.array(List.of(
                    RespValue.bulk(PMESSAGE),
                    RespValue.bulk(entry.getKey()),
                    RespValue.bulk(channel),
                    RespValue.bulk(message)));
            for (ClientSession subscriber : entry.getValue()) {
                if (deliver(subscriber, frame)) {
                    delivered++;
                } else {
                    entry.getValue().remove(subscriber);
                }
            }
        }
        return delivered;
    }

    private boolean deliver(ClientSession subscriber, RespValue frame) {
        if (!subscriber.output().isOpen()) {
            return false;
        }
        subscriber.push(frame);
        return true;
    }

    /** Drops every subscription held by a disconnecting client. */
    public void removeAll(ClientSession session) {
        for (String channel : List.copyOf(session.channels())) {
            unsubscribe(session, channel);
        }
        for (String pattern : List.copyOf(session.patterns())) {
            punsubscribe(session, pattern);
        }
    }

    /** PUBSUB CHANNELS: active channels with at least one subscriber. */
    public List<String> activeChannels(String pattern) {
        List<String> active = new ArrayList<>();
        for (Map.Entry<String, Set<ClientSession>> entry : channels.entrySet()) {
            if (entry.getValue().isEmpty()) {
                continue;
            }
            if (pattern == null || GlobPattern.matches(pattern, entry.getKey())) {
                active.add(entry.getKey());
            }
        }
        return active;
    }

    public int subscriberCount(String channel) {
        Set<ClientSession> subscribers = channels.get(channel);
        return subscribers == null ? 0 : subscribers.size();
    }

    public int patternCount() {
        return patterns.size();
    }
}
