package com.rishi.redis.store;

import com.rishi.redis.RedisException;

/** Raised when a command is applied to a key holding a different Redis type. */
public class WrongTypeException extends RedisException {

    public WrongTypeException() {
        super("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
}
