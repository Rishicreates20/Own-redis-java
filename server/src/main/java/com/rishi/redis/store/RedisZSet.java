package com.rishi.redis.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * TD-5 — a sorted set.
 *
 * <p>A ZSET has to answer two very different questions cheaply: "what score does
 * this member have" (hash lookup) and "give me members 10..20 in score order"
 * (ordered traversal). Redis solves it with a hash table beside a skip list; the
 * JVM equivalent is a {@link ConcurrentHashMap} beside a
 * {@link ConcurrentSkipListMap} keyed by {@code (score, member)}.
 *
 * <p>Reads run fully concurrently on both structures. Mutations are synchronized
 * <em>per sorted set</em>, because a score update is two structures moving together
 * and a torn update would leave the index disagreeing with the scores. Contention
 * is therefore scoped to a single key, never the keyspace.
 */
public final class RedisZSet {

    /** A skip-list key: score first, then member, so equal scores order lexicographically. */
    public record ScoredMember(double score, String member) implements Comparable<ScoredMember> {
        @Override
        public int compareTo(ScoredMember other) {
            int byScore = Double.compare(score, other.score);
            return byScore != 0 ? byScore : member.compareTo(other.member);
        }
    }

    private final ConcurrentHashMap<String, Double> scores = new ConcurrentHashMap<>();
    private final ConcurrentSkipListMap<ScoredMember, Boolean> index = new ConcurrentSkipListMap<>();

    /** @return true when the member was added rather than updated */
    public synchronized boolean add(String member, double score) {
        Double previous = scores.put(member, score);
        if (previous != null) {
            index.remove(new ScoredMember(previous, member));
        }
        index.put(new ScoredMember(score, member), Boolean.TRUE);
        return previous == null;
    }

    public synchronized double incrementBy(String member, double delta) {
        Double previous = scores.get(member);
        double updated = (previous == null ? 0.0 : previous) + delta;
        if (Double.isNaN(updated)) {
            throw new com.rishi.redis.RedisException("ERR resulting score is not a number (NaN)");
        }
        add(member, updated);
        return updated;
    }

    public synchronized boolean remove(String member) {
        Double previous = scores.remove(member);
        if (previous == null) {
            return false;
        }
        index.remove(new ScoredMember(previous, member));
        return true;
    }

    public synchronized ScoredMember pollFirst() {
        Map.Entry<ScoredMember, Boolean> entry = index.pollFirstEntry();
        if (entry == null) {
            return null;
        }
        scores.remove(entry.getKey().member());
        return entry.getKey();
    }

    public synchronized ScoredMember pollLast() {
        Map.Entry<ScoredMember, Boolean> entry = index.pollLastEntry();
        if (entry == null) {
            return null;
        }
        scores.remove(entry.getKey().member());
        return entry.getKey();
    }

    public Double score(String member) {
        return scores.get(member);
    }

    public int size() {
        return scores.size();
    }

    public boolean isEmpty() {
        return scores.isEmpty();
    }

    /** Every member in score order; the caller may reverse it. */
    public List<ScoredMember> all() {
        return new ArrayList<>(index.keySet());
    }

    /**
     * ZRANGE semantics: inclusive indexes, negatives counting back from the end,
     * out-of-range values clamped rather than rejected.
     */
    public List<ScoredMember> range(long start, long stop, boolean reverse) {
        List<ScoredMember> ordered = all();
        if (reverse) {
            Collections.reverse(ordered);
        }
        int n = ordered.size();
        long from = start < 0 ? n + start : start;
        long to = stop < 0 ? n + stop : stop;
        if (from < 0) {
            from = 0;
        }
        if (to >= n) {
            to = n - 1L;
        }
        if (from > to || from >= n) {
            return List.of();
        }
        return new ArrayList<>(ordered.subList((int) from, (int) to + 1));
    }

    /** ZRANGEBYSCORE: a half-open/closed interval walk straight off the skip list. */
    public List<ScoredMember> rangeByScore(ScoreBound min, ScoreBound max, boolean reverse,
                                           long offset, long count) {
        ScoredMember low = new ScoredMember(min.value(), "");
        // Wire strings are ISO-8859-1, so this sentinel orders after any real
        // member holding the same score.
        ScoredMember high = new ScoredMember(max.value(), "\uffff");
        List<ScoredMember> matched = new ArrayList<>();
        for (ScoredMember candidate : index.subMap(low, true, high, true).keySet()) {
            if (min.excludes(candidate.score(), true) || max.excludes(candidate.score(), false)) {
                continue;
            }
            matched.add(candidate);
        }
        if (reverse) {
            Collections.reverse(matched);
        }
        if (offset > 0) {
            if (offset >= matched.size()) {
                return List.of();
            }
            matched = matched.subList((int) offset, matched.size());
        }
        if (count >= 0 && count < matched.size()) {
            matched = matched.subList(0, (int) count);
        }
        return new ArrayList<>(matched);
    }

    public long count(ScoreBound min, ScoreBound max) {
        return rangeByScore(min, max, false, 0, -1).size();
    }

    /** 0-based position in score order, or {@code null} when absent. */
    public Long rank(String member, boolean reverse) {
        Double score = scores.get(member);
        if (score == null) {
            return null;
        }
        long position = index.headMap(new ScoredMember(score, member), false).size();
        return reverse ? (long) size() - position - 1 : position;
    }

    /** One end of a ZRANGEBYSCORE interval: {@code 5}, {@code (5}, {@code -inf}, {@code +inf}. */
    public record ScoreBound(double value, boolean exclusive) {

        public static ScoreBound parse(String text) {
            boolean exclusive = text.startsWith("(");
            String body = exclusive ? text.substring(1) : text;
            double value = switch (body.toLowerCase()) {
                case "+inf", "inf" -> Double.POSITIVE_INFINITY;
                case "-inf" -> Double.NEGATIVE_INFINITY;
                default -> {
                    try {
                        yield Double.parseDouble(body);
                    } catch (NumberFormatException e) {
                        throw new com.rishi.redis.RedisException("ERR min or max is not a float");
                    }
                }
            };
            return new ScoreBound(value, exclusive);
        }

        /** @param isLowerBound whether this bound is the minimum of the interval */
        public boolean excludes(double score, boolean isLowerBound) {
            if (isLowerBound) {
                return exclusive ? score <= value : score < value;
            }
            return exclusive ? score >= value : score > value;
        }
    }
}
