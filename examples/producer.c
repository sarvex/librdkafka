/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2017, Magnus Edenhill
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

/**
 * Simple Apache Kafka producer
 * using the Kafka driver from librdkafka
 * (https://github.com/edenhill/librdkafka)
 */

#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <stdlib.h>


/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is builtin from within the librdkafka source tree and thus differs. */
#include "rdkafka.h"

static int msg_sent      = 0;
static int msg_delivered = 0;

static volatile sig_atomic_t run = 1;

/**
 * @brief Signal termination of program
 */
static void stop(int sig) {
        run = 0;
        fclose(stdin); /* abort fgets() */
}


/**
 * @brief Message delivery report callback.
 *
 * This callback is called exactly once per message, indicating if
 * the message was succesfully delivered
 * (rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR) or permanently
 * failed delivery (rkmessage->err != RD_KAFKA_RESP_ERR_NO_ERROR).
 *
 * The callback is triggered from rd_kafka_poll() and executes on
 * the application's thread.
 */
static void
dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) {
        if (rkmessage->err)
                fprintf(stderr, "%% Message delivery failed: %s\n",
                        rd_kafka_err2str(rkmessage->err));
        else {
                // fprintf(stderr,
                //         "%% Message delivered (%zd bytes, "
                //         "partition %" PRId32 ")\n",
                // rkmessage->len, rkmessage->partition);
                msg_delivered += 1;
        }

        /* The rkmessage is destroyed automatically by librdkafka */
}


int32_t *test_get_broker_ids(rd_kafka_t *use_rk, size_t *cntp) {
        int32_t *ids;
        rd_kafka_t *rk = use_rk;
        const rd_kafka_metadata_t *md;
        rd_kafka_resp_err_t err;
        size_t i;

        err = rd_kafka_metadata(rk, 0, NULL, &md, (5000));

        ids = malloc(sizeof(*ids) * md->broker_cnt);

        for (i = 0; i < (size_t)md->broker_cnt; i++)
                ids[i] = md->brokers[i].id;

        *cntp = md->broker_cnt;

        rd_kafka_metadata_destroy(md);

        if (!use_rk)
                rd_kafka_destroy(rk);

        return ids;
}


int main(int argc, char **argv) {
        rd_kafka_t *rk;        /* Producer instance handle */
        rd_kafka_conf_t *conf; /* Temporary configuration object */
        char errstr[512];      /* librdkafka API error reporting buffer */
        const char *brokers;   /* Argument: broker list */
        const char *topic;     /* Argument: topic to produce to */

        /*
         * Argument validation
         */
        if (argc != 3) {
                fprintf(stderr, "%% Usage: %s <broker> <topic>\n", argv[0]);
                return 1;
        }

        brokers = argv[1];
        topic   = argv[2];


        /*
         * Create Kafka client configuration place-holder
         */
        conf = rd_kafka_conf_new();

        /* Set bootstrap broker(s) as a comma-separated list of
         * host or host:port (default port 9092).
         * librdkafka will use the bootstrap brokers to acquire the full
         * set of brokers from the cluster. */
        if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr,
                              sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                fprintf(stderr, "%s\n", errstr);
                return 1;
        }

        /* Set the delivery report callback.
         * This callback will be called once per message to inform
         * the application if delivery succeeded or failed.
         * See dr_msg_cb() above.
         * The callback is only triggered from rd_kafka_poll() and
         * rd_kafka_flush(). */
        rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);

        rd_kafka_conf_set(conf, "sasl.mechanism", "PLAIN", errstr,
                          sizeof(errstr));
        rd_kafka_conf_set(conf, "security.protocol", "SASL_PLAINTEXT", errstr,
                          sizeof(errstr));
        rd_kafka_conf_set(conf, "sasl.username", "broker", errstr,
                          sizeof(errstr));
        rd_kafka_conf_set(conf, "sasl.password", "broker", errstr,
                          sizeof(errstr));
        rd_kafka_conf_set(conf, "debug", "security", errstr, sizeof(errstr));

        /*
         * Create producer instance.
         *
         * NOTE: rd_kafka_new() takes ownership of the conf object
         *       and the application must not reference it again after
         *       this call.
         */
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!rk) {
                fprintf(stderr, "%% Failed to create new producer: %s\n",
                        errstr);
                return 1;
        }

        /* Signal handler for clean shutdown */
        signal(SIGINT, stop);

        size_t bcount;
        int32_t *ids = test_get_broker_ids(rk, &bcount);

        fprintf(stderr, "MILIND::id = %d\n", ids[0]);

        rd_kafka_queue_t *queue = rd_kafka_queue_new(rk);

        rd_kafka_ConfigResource_t *configs =
            rd_kafka_ConfigResource_new(RD_KAFKA_RESOURCE_BROKER, "12");
        rd_kafka_DescribeConfigs(rk, &configs, 1, NULL, queue);

        rd_kafka_event_t *event =
            rd_kafka_queue_poll(queue, -1 /* infinite timeout */);

        const rd_kafka_ConfigResource_t **results;
        size_t result_cnt;
        results = rd_kafka_DescribeConfigs_result_resources(
            rd_kafka_event_DescribeConfigs_result(event), &result_cnt);

        if (result_cnt != 1) {
                fprintf(stderr, "Got no result!!!\n");
        }

        const rd_kafka_ConfigEntry_t **configents;
        size_t configent_cnt;
        configents =
            rd_kafka_ConfigResource_configs(results[0], &configent_cnt);

        for (int i = 0; i < configent_cnt; i++) {
                const char *entry_name =
                    rd_kafka_ConfigEntry_name(configents[i]);
                const char *entry_value =
                    rd_kafka_ConfigEntry_value(configents[i]);
                fprintf(stderr, "MILIND::config %s, value %s\n", entry_name,
                        entry_value);
        }


        while (run /*&& fgets(buf, sizeof(buf), stdin)*/) {
                char buf[] = "hello"; /* Message value temporary buffer */
                size_t len = strlen(buf);
                rd_kafka_resp_err_t err;

                if (buf[len - 1] == '\n') /* Remove newline */
                        buf[--len] = '\0';

                if (len == 0) {
                        /* Empty line: only serve delivery reports */
                        rd_kafka_poll(rk, 0 /*non-blocking */);
                        continue;
                }

                /*
                 * Send/Produce message.
                 * This is an asynchronous call, on success it will only
                 * enqueue the message on the internal producer queue.
                 * The actual delivery attempts to the broker are handled
                 * by background threads.
                 * The previously registered delivery report callback
                 * (dr_msg_cb) is used to signal back to the application
                 * when the message has been delivered (or failed).
                 */
        retry:
                err = rd_kafka_producev(
                    /* Producer handle */
                    rk,
                    /* Topic name */
                    RD_KAFKA_V_TOPIC(topic),
                    /* Make a copy of the payload. */
                    RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                    /* Message value and length */
                    RD_KAFKA_V_VALUE(buf, len),
                    /* Per-Message opaque, provided in
                     * delivery report callback as
                     * msg_opaque. */
                    RD_KAFKA_V_OPAQUE(NULL),
                    /* End sentinel */
                    RD_KAFKA_V_END);

                if (err) {
                        /*
                         * Failed to *enqueue* message for producing.
                         */
                        fprintf(stderr,
                                "%% Failed to produce to topic %s: %s\n", topic,
                                rd_kafka_err2str(err));

                        if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                                /* If the internal queue is full, wait for
                                 * messages to be delivered and then retry.
                                 * The internal queue represents both
                                 * messages to be sent and messages that have
                                 * been sent or failed, awaiting their
                                 * delivery report callback to be called.
                                 *
                                 * The internal queue is limited by the
                                 * configuration property
                                 * queue.buffering.max.messages and
                                 * queue.buffering.max.kbytes */
                                rd_kafka_poll(rk,
                                              1000 /*block for max 1000ms*/);
                                goto retry;
                        }
                } else {
                        msg_sent += 1;
                        // fprintf(stderr,
                        //         "%% Enqueued message (%zd bytes) "
                        //         "for topic %s\n",
                        //         len, topic);
                }


                /* A producer application should continually serve
                 * the delivery report queue by calling rd_kafka_poll()
                 * at frequent intervals.
                 * Either put the poll call in your main loop, or in a
                 * dedicated thread, or call it after every
                 * rd_kafka_produce() call.
                 * Just make sure that rd_kafka_poll() is still called
                 * during periods where you are not producing any messages
                 * to make sure previously produced messages have their
                 * delivery report callback served (and any other callbacks
                 * you register). */
                rd_kafka_poll(rk, 0 /*non-blocking*/);
        }


        /* Wait for final messages to be delivered or fail.
         * rd_kafka_flush() is an abstraction over rd_kafka_poll() which
         * waits for all messages to be delivered. */
        fprintf(stderr, "%% Flushing final messages..\n");
        rd_kafka_flush(rk, 10 * 1000 /* wait for max 10 seconds */);

        /* If the output queue is still not empty there is an issue
         * with producing messages to the clusters. */
        if (rd_kafka_outq_len(rk) > 0)
                fprintf(stderr, "%% %d message(s) were not delivered\n",
                        rd_kafka_outq_len(rk));

        /* Destroy the producer instance */
        rd_kafka_destroy(rk);

        fprintf(stderr, "Stats %d vs %d\n", msg_delivered, msg_sent);

        return 0;
}
