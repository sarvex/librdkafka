/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2023, Confluent Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "test.h"
/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is built from within the librdkafka source tree and thus differs. */
#include "rdkafka.h" /* for Kafka driver */

static int sent_msg = 0;
static int delivered_msg = 0;

static void dr_msg_cb(rd_kafka_t *rk,
                    const rd_kafka_message_t *rkmessage,
                    void *opaque) {
        if (rkmessage->err)
            TEST_FAIL("Message delivery failed: %s\n", rd_kafka_err2str(rkmessage->err));
        else {
            delivered_msg++;
        }
}

// TODO: Add a test based on txn producer, and a test with a consumer.

// void do_test_txn_producer(int64_t reauth_time, const char *topic) {
//     int64_t start_micros = test_clock();
//     rd_kafka_conf_t *conf = NULL;
//     rd_kafka_t *rk = NULL;
//     uint64_t testid =  test_id_generate();
//     rd_kafka_resp_err_t err;

//     test_timeout_set(reauth_time/1000 + 30);
//     SUB_TEST("test reauth in the middle of a transaction");

//     test_conf_init(&conf, NULL, 30);
//     test_conf_set(conf, "transactional.id", topic);
//     test_conf_set(conf, "transaction.timeout.ms", tsprintf("%ld", reauth_time + 2000));
//     rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);

//     rk = test_create_handle(RD_KAFKA_PRODUCER, conf);

//     test_create_topic(rk, topic, 1, 1);
//     // TEST_ASSERT(!err, "topic creation failed: %s", rd_kafka_err2str(err));

//     delivered_msg = 0;
//     sent_msg = 0;

//     TEST_CALL_ERROR__(rd_kafka_init_transactions(rk, -1));
//     TEST_CALL_ERROR__(rd_kafka_begin_transaction(rk));

//     test_produce_msgs2_nowait(
//                                     rk, topic, testid,
//                                     RD_KAFKA_PARTITION_UA, 0, 1,
//                                     NULL, 0, &sent_msg);

//     while ((test_clock() - start_micros) < reauth_time * 1000) {
//         // Don't do anything.
//         rd_kafka_poll(rk, 1000);
//     }

//     TEST_CALL_ERROR__(rd_kafka_commit_transaction(rk, 30 * 1000));
//     TEST_ASSERT(delivered_msg == sent_msg, "did not deliver as many messages as sent (%d vs %d)", delivered_msg, sent_msg);

//     rd_kafka_destroy(rk);

//     SUB_TEST_PASS();

// }

void do_test_producer(int64_t reauth_time, const char *topic) {
    rd_kafka_conf_t *conf = NULL;
    rd_kafka_topic_t *rkt = NULL;
    rd_kafka_t *rk = NULL;
    uint64_t testid =  test_id_generate();
    rd_kafka_resp_err_t err;
    int msgrate, msgcnt;
    test_timing_t t_produce;

    msgrate = 200; /* msg/sec */
    /* Messages should be produced such that at least one reauth happens. The 1.2 is added as a buffer to avoid flakiness. */
    msgcnt = msgrate * reauth_time/1000 * 1.2;
    delivered_msg = 0;
    sent_msg = 0;

    test_timeout_set(reauth_time/1000 + 30);

    SUB_TEST("test producer message loss while reauthenticating");

    test_conf_init(&conf, NULL, 30);
    rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);

    rk = test_create_handle(RD_KAFKA_PRODUCER, conf);
    rkt = test_create_producer_topic(rk, topic, NULL);

    /* Create the topic to make sure connections are up and ready. */
    err = test_auto_create_topic_rkt(rk, rkt, tmout_multip(5000));
    TEST_ASSERT(!err, "topic creation failed: %s", rd_kafka_err2str(err));

    TIMING_START(&t_produce, "PRODUCE");
    /* Produce enough messages such that we have time enough for at least one reauth. */
    test_produce_msgs_nowait(rk, rkt, testid, 0, 0, msgcnt, NULL, 0, msgrate, &sent_msg);
    TIMING_STOP(&t_produce);

    rd_kafka_flush(rk, 10*1000);

    TEST_ASSERT(TIMING_DURATION(&t_produce) >= reauth_time * 1000, "time enough for one reauth should pass (%ld vs %ld)", TIMING_DURATION(&t_produce), reauth_time * 1000);
    TEST_ASSERT(delivered_msg == sent_msg, "did not deliver as many messages as sent (%d vs %d)", delivered_msg, sent_msg);

    rd_kafka_topic_destroy(rkt);
    rd_kafka_destroy(rk);

    SUB_TEST_PASS();
}


int main_0139_reauthentication(int argc, char **argv) {
    size_t broker_id_cnt;
    int32_t *broker_ids = NULL;
    rd_kafka_t *rk = test_create_producer();
    const char *security_protocol = test_conf_get(NULL, "security.protocol");
    size_t i;
    int64_t reauth_time = INT64_MAX;
    const char *topic = test_mk_topic_name(__FUNCTION__ + 5, 1);

    if (strncmp(security_protocol, "sasl", 4)) {
        TEST_SKIP("Test requires SASL_PLAINTEXT or SASL_SSL\n");
        return 0;
    }

    broker_ids = test_get_broker_ids(rk, &broker_id_cnt);

    TEST_ASSERT(broker_id_cnt != 0);

    for (i = 0; i < broker_id_cnt; i++) {
        char *property_value = test_get_broker_config_entry(rk, broker_ids[i], "connections.max.reauth.ms");
        int64_t parsed_value;

        if (!property_value)
            continue;

        parsed_value = strtoll(property_value, NULL, 0);
        if (parsed_value < reauth_time)
            reauth_time = parsed_value;

        free(property_value);
    }

    if (broker_ids)
        free(broker_ids);
    if (rk)
        rd_kafka_destroy(rk);

    if (reauth_time == INT64_MAX /* denotes property is unset on all brokers */
        || reauth_time == 0 /* denotes at least one broker without timeout */
    ) {
        TEST_SKIP("Test requires all brokers to have non-zero connections.max.reauth.ms");
        return 0;
    }

    do_test_producer(reauth_time, topic);
    // do_test_txn_producer(reauth_time, topic);

    return 0;
}
