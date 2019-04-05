extern crate redis;

use redis::{
    Commands, Connection, RedisResult, StreamClaimOptions, StreamClaimReply,
    StreamInfoConsumersReply, StreamInfoGroupsReply, StreamInfoStreamsReply, StreamMaxlen,
    StreamPendingCountReply, StreamPendingReply, StreamRangeReply, StreamReadOptions,
    StreamReadReply, ToRedisArgs,
};

use std::collections::HashMap;
use std::str;
use std::thread::{sleep, spawn};
use std::time::Duration;

use support::*;

mod support;

macro_rules! assert_args {
    ($value:expr, $($args:expr),+) => {
        let args = $value.to_redis_args();
        let strings: Vec<_> = args.iter()
                                .map(|a| str::from_utf8(a.as_ref()).unwrap())
                                .collect();
        assert_eq!(strings, vec![$($args),+]);
    }
}

fn xadd(con: &mut Connection) {
    let _: RedisResult<String> =
        con.xadd("k1", "1000-0", &[("hello", "world"), ("redis", "streams")]);
    let _: RedisResult<String> = con.xadd("k1", "1000-1", &[("hello", "world2")]);
    let _: RedisResult<String> = con.xadd("k2", "2000-0", &[("hello", "world")]);
    let _: RedisResult<String> = con.xadd("k2", "2000-1", &[("hello", "world2")]);
}

fn xadd_keyrange(con: &mut Connection, key: &str, start: i32, end: i32) {
    for _i in start..end {
        let _: RedisResult<String> = con.xadd(key, "*", &[("h", "w")]);
    }
}

#[test]
fn test_cmd_options() {
    // Tests the following command option builders....
    // xclaim_option
    // xread_option
    // maxlen enum

    // test read options

    let empty = StreamClaimOptions::default();
    assert_eq!(ToRedisArgs::to_redis_args(&empty).len(), 0);

    let empty = StreamReadOptions::default();
    assert_eq!(ToRedisArgs::to_redis_args(&empty).len(), 0);

    let opts = StreamClaimOptions::default()
        .idle(50)
        .time(500)
        .retry(3)
        .with_force()
        .with_justid();

    assert_args!(
        &opts,
        "IDLE",
        "50",
        "TIME",
        "500",
        "RETRYCOUNT",
        "3",
        "FORCE",
        "JUSTID"
    );

    // test maxlen options

    assert_args!(StreamMaxlen::Aprrox(10), "MAXLEN", "~", "10");
    assert_args!(StreamMaxlen::Equals(10), "MAXLEN", "=", "10");

    // test read options

    let opts = StreamReadOptions::default()
        .block(100)
        .count(200)
        .group("group-name", "consumer-name");

    assert_args!(
        &opts,
        "BLOCK",
        "100",
        "COUNT",
        "200",
        "GROUP",
        "group-name",
        "consumer-name"
    );
}

#[test]
fn test_assorted_1() {
    // Tests the following commands....
    // xadd
    // xadd_map
    // xadd_maxlen
    // xread
    // xlen

    let ctx = TestContext::new();
    let mut con = ctx.connection();

    xadd(&mut con);

    // smoke test we get the same id back
    let result: RedisResult<String> = con.xadd("k0", "1000-0", &[("x", "y")]);
    assert_eq!(result.unwrap(), "1000-0");

    // xread reply
    let reply: StreamReadReply = con.xread(&["k1", "k2", "k3"], &["0", "0", "0"]).unwrap();

    // verify reply contains 2 keys even though we asked for 3
    assert_eq!(&reply.keys.len(), &2usize);

    // verify first key & first id exist
    assert_eq!(&reply.keys[0].key, "k1");
    assert_eq!(&reply.keys[0].ids.len(), &2usize);
    assert_eq!(&reply.keys[0].ids[0].id, "1000-0");

    // lookup the key in StreamId map
    let hello: Option<String> = reply.keys[0].ids[0].get("hello");
    assert_eq!(hello, Some("world".to_string()));

    // verify the second key was written
    assert_eq!(&reply.keys[1].key, "k2");
    assert_eq!(&reply.keys[1].ids.len(), &2usize);
    assert_eq!(&reply.keys[1].ids[0].id, "2000-0");

    // test xadd_map
    let mut map: HashMap<&str, &str> = HashMap::new();
    map.insert("ab", "cd");
    map.insert("ef", "gh");
    map.insert("ij", "kl");
    let _: RedisResult<String> = con.xadd_map("k3", "3000-0", map);

    let reply: StreamRangeReply = con.xrange_all("k3").unwrap();
    assert_eq!(reply.ids[0].contains_key(&"ab"), true);
    assert_eq!(reply.ids[0].contains_key(&"ef"), true);
    assert_eq!(reply.ids[0].contains_key(&"ij"), true);

    // test xadd w/ maxlength below...

    // add 100 things to k4
    xadd_keyrange(&mut con, "k4", 0, 100);

    // test xlen.. should have 100 items
    let result: RedisResult<usize> = con.xlen("k4");
    assert_eq!(result, Ok(100));

    // test xadd_maxlen
    let result: RedisResult<String> =
        con.xadd_maxlen("k4", StreamMaxlen::Equals(10), "*", &[("h", "w")]);
    let result: RedisResult<usize> = con.xlen("k4");
    assert_eq!(result, Ok(10));
}

#[test]
fn test_assorted_2() {
    // Tests the following commands....
    // xadd
    // xinfo_stream
    // xinfo_groups
    // xinfo_consumer
    // xgroup_create
    // xgroup_create_mkstream
    // xread_options
    // xack
    // xpending
    // xpending_count
    // xpending_consumer_count

    let ctx = TestContext::new();
    let mut con = ctx.connection();

    xadd(&mut con);

    // no key exists... this call breaks the connection pipe for some reason
    let reply: RedisResult<StreamInfoStreamsReply> = con.xinfo_stream("k10");
    assert_eq!(reply.is_err(), true);

    // redo the connection because the above error
    con = ctx.connection();

    // key should exist
    let reply: StreamInfoStreamsReply = con.xinfo_stream("k1").unwrap();
    assert_eq!(&reply.first_entry.id, "1000-0");
    assert_eq!(&reply.last_entry.id, "1000-1");
    assert_eq!(&reply.last_generated_id, "1000-1");

    // xgroup create (existing stream)
    let result: RedisResult<String> = con.xgroup_create("k1", "g1", "$");
    assert_eq!(result.is_ok(), true);

    // xinfo groups (existing stream)
    let result: RedisResult<StreamInfoGroupsReply> = con.xinfo_groups("k1");
    assert_eq!(result.is_ok(), true);
    let reply = result.unwrap();
    assert_eq!(&reply.groups.len(), &1);
    assert_eq!(&reply.groups[0].name, &"g1");

    // test xgroup create w/ mkstream @ 0
    let result: RedisResult<String> = con.xgroup_create_mkstream("k99", "g99", "0");
    assert_eq!(result.is_ok(), true);

    // Since nothing exists on this stream yet,
    // it should have the defaults returned by the client
    let result: RedisResult<StreamInfoGroupsReply> = con.xinfo_groups("k99");
    assert_eq!(result.is_ok(), true);
    let reply = result.unwrap();
    assert_eq!(&reply.groups.len(), &1);
    assert_eq!(&reply.groups[0].name, &"g99");
    assert_eq!(&reply.groups[0].last_delivered_id, &"0-0");

    // call xadd on k99 just so we can read from it
    // using consumer g99 and test xinfo_consumers
    let _: RedisResult<String> = con.xadd("k99", "1000-0", &[("a", "b"), ("c", "d")]);
    let _: RedisResult<String> = con.xadd("k99", "1000-1", &[("e", "f"), ("g", "h")]);

    // passing options  w/ group triggers XREADGROUP
    // using ID=">" means all undelivered ids
    // otherwise, ID="0 | ms-num" means all pending already
    // sent to this client
    let reply: StreamReadReply = con
        .xread_options(
            &["k99"],
            &[">"],
            StreamReadOptions::default().group("g99", "c99"),
        )
        .unwrap();
    assert_eq!(reply.keys[0].ids.len(), 2);

    // read xinfo consumers again, should have 2 messages for the c99 consumer
    let reply: StreamInfoConsumersReply = con.xinfo_consumers("k99", "g99").unwrap();
    assert_eq!(reply.consumers[0].pending, 2);

    // ack one of these messages
    let result: RedisResult<i32> = con.xack("k99", "g99", &["1000-0"]);
    assert_eq!(result, Ok(1));

    // get pending messages already seen by this client
    // we should only have one now..
    let reply: StreamReadReply = con
        .xread_options(
            &["k99"],
            &["0"],
            StreamReadOptions::default().group("g99", "c99"),
        )
        .unwrap();
    assert_eq!(reply.keys.len(), 1);

    // we should also have one pending here...
    let reply: StreamInfoConsumersReply = con.xinfo_consumers("k99", "g99").unwrap();
    assert_eq!(reply.consumers[0].pending, 1);

    // add more and read so we can test xpending
    let _: RedisResult<String> = con.xadd("k99", "1001-0", &[("i", "j"), ("k", "l")]);
    let _: RedisResult<String> = con.xadd("k99", "1001-1", &[("m", "n"), ("o", "p")]);
    let _: StreamReadReply = con
        .xread_options(
            &["k99"],
            &[">"],
            StreamReadOptions::default().group("g99", "c99"),
        )
        .unwrap();

    // call xpending here...
    // this has a different reply from what the count variations return
    let reply: StreamPendingReply = con.xpending("k99", "g99").unwrap();
    assert_eq!(reply.count, 3);
    assert_eq!(reply.start_id, "1000-1");
    assert_eq!(reply.end_id, "1001-1");
    assert_eq!(reply.consumers.len(), 1);
    assert_eq!(reply.consumers[0].name, "c99");

    // both count variations have the same reply types
    let reply: StreamPendingCountReply = con.xpending_count("k99", "g99", "-", "+", 10).unwrap();
    assert_eq!(reply.ids.len(), 3);

    let reply: StreamPendingCountReply = con
        .xpending_consumer_count("k99", "g99", "-", "+", 10, "c99")
        .unwrap();
    assert_eq!(reply.ids.len(), 3);

    // test mixmatched StreamPending* reply types..

    // xpending => StreamPendingCountReply
    // this one doesn't throw an error. the ids will be empty, though.
    let result: RedisResult<StreamPendingCountReply> = con.xpending("k99", "g99");
    assert_eq!(result.unwrap().ids.len(), 0);

    // xpending_count => StreamPendingReply
    // this throws an error in the FromRedisValue trait
    let result: RedisResult<StreamPendingReply> = con.xpending_count("k99", "g99", "-", "+", 10);
    assert_eq!(result.is_err(), true);
}

#[test]
fn test_xclaim() {
    // Tests the following commands....
    // xclaim
    // xclaim_options
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    // xclaim test basic idea:
    // 1. we need to test adding messages to a group
    // 2. then xreadgroup needs to define a consumer and read pending
    //    messages without acking them
    // 3. then we need to sleep 5ms and call xpending
    // 4. from here we should be able to claim message
    //    past the idle time and read them from a different consumer

    // create the group
    let result: RedisResult<String> = con.xgroup_create_mkstream("k1", "g1", "$");
    assert_eq!(result.is_ok(), true);

    // add some keys
    xadd_keyrange(&mut con, "k1", 0, 10);

    // read the pending items for this key & group
    let reply: StreamReadReply = con
        .xread_options(
            &["k1"],
            &[">"],
            StreamReadOptions::default().group("g1", "c1"),
        )
        .unwrap();
    // verify we have 10 ids
    assert_eq!(reply.keys[0].ids.len(), 10);

    // save this StreamId for later
    let claim = &reply.keys[0].ids[0];
    let claim_1 = &reply.keys[0].ids[1];
    let claim_justids = &reply.keys[0].just_ids();

    // sleep for 5ms
    sleep(Duration::from_millis(5));

    // grab this id if > 4ms
    let reply: StreamClaimReply = con
        .xclaim("k1", "g1", "c2", 4, &[claim.id.clone()])
        .unwrap();
    assert_eq!(reply.ids.len(), 1);
    assert_eq!(reply.ids[0].id, claim.id);

    // grab all pending ids for this key...
    // we should 9 in c1 and 1 in c2
    let reply: StreamPendingReply = con.xpending("k1", "g1").unwrap();
    assert_eq!(reply.consumers[0].name, "c1");
    assert_eq!(reply.consumers[0].pending, 9);
    assert_eq!(reply.consumers[1].name, "c2");
    assert_eq!(reply.consumers[1].pending, 1);

    // sleep for 5ms
    sleep(Duration::from_millis(5));

    // lets test some of the xclaim_options
    // call force on the same claim.id
    let reply: StreamClaimReply = con
        .xclaim_options(
            "k1",
            "g1",
            "c3",
            4,
            &[claim.id.clone()],
            StreamClaimOptions::default().with_force(),
        )
        .unwrap();

    let reply: StreamPendingReply = con.xpending("k1", "g1").unwrap();
    // we should have 9 w/ c1 and 1 w/ c3 now
    assert_eq!(reply.consumers[1].name, "c3");
    assert_eq!(reply.consumers[1].pending, 1);

    // sleep for 5ms
    sleep(Duration::from_millis(5));

    // claim and only return JUSTID
    let claimed: Vec<String> = con
        .xclaim_options(
            "k1",
            "g1",
            "c5",
            4,
            &claim_justids,
            StreamClaimOptions::default().with_force().with_justid(),
        )
        .unwrap();
    // we just claimed the origin 10 ids
    // and only returned the ids
    assert_eq!(claimed.len(), 10);
}

#[test]
fn test_xdel() {
    // Tests the following commands....
    // xdel
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    // add some keys
    xadd(&mut con);

    // delete the first stream item for this key
    let result: RedisResult<i32> = con.xdel("k1", &["1000-0"]);
    // returns the number of items deleted
    assert_eq!(result, Ok(1));

    let result: RedisResult<i32> = con.xdel("k2", &["2000-0", "2000-1", "2000-2"]);
    // should equal 2 since the last id doesn't exist
    assert_eq!(result, Ok(2));
}

#[test]
fn test_xgroup() {
    // Tests the following commands....
    // xgroup_create_mkstream
    // xgroup_destroy
    // xgroup_delconsumer

    let ctx = TestContext::new();
    let mut con = ctx.connection();

    // test xgroup create w/ mkstream @ 0
    let result: RedisResult<String> = con.xgroup_create_mkstream("k1", "g1", "0");
    assert_eq!(result.is_ok(), true);

    // destroy this new stream group
    let result: RedisResult<i32> = con.xgroup_destroy("k1", "g1");
    assert_eq!(result, Ok(1));

    // add some keys
    xadd(&mut con);

    // create the group again using an existing stream
    let result: RedisResult<String> = con.xgroup_create("k1", "g1", "0");
    assert_eq!(result.is_ok(), true);

    // read from the group so we can register the consumer
    let reply: StreamReadReply = con
        .xread_options(
            &["k1"],
            &[">"],
            StreamReadOptions::default().group("g1", "c1"),
        )
        .unwrap();
    assert_eq!(reply.keys[0].ids.len(), 2);

    let result: RedisResult<i32> = con.xgroup_delconsumer("k1", "g1", "c1");
    // returns the number of pending message this client had open
    assert_eq!(result, Ok(2));

    let result: RedisResult<i32> = con.xgroup_destroy("k1", "g1");
    assert_eq!(result, Ok(1));
}

#[test]
fn test_xrange() {
    // Tests the following commands....
    // xrange (-/+ variations)
    // xrange_all
    // xrange_count

    let ctx = TestContext::new();
    let mut con = ctx.connection();

    xadd(&mut con);

    // xrange replies
    let reply: StreamRangeReply = con.xrange_all("k1").unwrap();
    assert_eq!(reply.ids.len(), 2);

    let reply: StreamRangeReply = con.xrange("k1", "1000-1", "+").unwrap();
    assert_eq!(reply.ids.len(), 1);

    let reply: StreamRangeReply = con.xrange("k1", "-", "1000-0").unwrap();
    assert_eq!(reply.ids.len(), 1);

    let reply: StreamRangeReply = con.xrange_count("k1", "-", "+", 1).unwrap();
    assert_eq!(reply.ids.len(), 1);
}

#[test]
fn test_xrevrange() {
    // Tests the following commands....
    // xrevrange (+/- variations)
    // xrevrange_all
    // xrevrange_count

    let ctx = TestContext::new();
    let mut con = ctx.connection();

    xadd(&mut con);

    // xrange replies
    let reply: StreamRangeReply = con.xrevrange_all("k1").unwrap();
    assert_eq!(reply.ids.len(), 2);

    let reply: StreamRangeReply = con.xrevrange("k1", "1000-1", "-").unwrap();
    assert_eq!(reply.ids.len(), 2);

    let reply: StreamRangeReply = con.xrevrange("k1", "+", "1000-1").unwrap();
    assert_eq!(reply.ids.len(), 1);

    let reply: StreamRangeReply = con.xrevrange_count("k1", "+", "-", 1).unwrap();
    assert_eq!(reply.ids.len(), 1);
}