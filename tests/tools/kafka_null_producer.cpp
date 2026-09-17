/* kafka_null_producer - the records the component's own producer cannot express.
 *
 * KafkaProducer::Produce() always hands librdkafka MessageValue.c_str() with
 * MessageValue.size() (src/producer1c_core.cpp:349), so the payload pointer is
 * never NULL: a real Kafka tombstone - a record whose VALUE IS ABSENT, not
 * empty - cannot be produced through the component at all. The same is true of
 * a header whose value is absent, which is what RdKafka::Headers::Header::
 * value_string() == nullptr means on the consuming side and which ordinary
 * Kafka producers emit all the time.
 *
 * Those two are exactly the inputs finding 3 is about, so this little program
 * exists to put them in a topic. It talks to librdkafka directly and is a
 * separate EXECUTABLE rather than a function inside kafka_ssl_test for one
 * reason: the test process dlopen()s a component that already carries its own
 * statically linked copy of librdkafka, and linking a second copy into the same
 * process would let the two interpose each other's symbols. A fork/exec costs
 * nothing here and keeps the test process exactly as it is in every other case.
 *
 * Usage:
 *   kafka_null_producer <bootstrap> <ca.pem|-> <topic>
 *
 * "-" as the CA path means PLAINTEXT; anything else configures SSL with that CA.
 *
 * It writes four records, in this order and all to partition 0 (the test topics
 * have exactly one):
 *
 *   R1  "r1-tombstone"   value ABSENT (tombstone)
 *                        headers: "h-null" = ABSENT
 *                                 "h-empty" = present, zero length
 *                                 "" (EMPTY KEY) = "hv"
 *   R2  "r2-emptyvalue"  value present, zero length; header "h-ok" = 123
 *   R3  "" (EMPTY KEY)   value {"big":"ZZZ..."}; header "h-ok" = {"n":1}
 *   R4  key ABSENT       value {"r4":true}; no headers
 *
 * Every key / value / header key / header value that IS present is written as a
 * syntactically valid JSON token, quotes included in the bytes. That matters:
 * the test reads this topic back with all four Escape* properties set to False,
 * so the component copies these bytes into the pool document verbatim and the
 * only thing that can then make the document unparseable is the component
 * itself.
 *
 * Exit status: 0 when all four were delivered, 2 otherwise. The last line of
 * stdout is "ok <n>" with the number delivered, which is what the test reads.
 */

#include <stdio.h>
#include <string.h>

#include "rdkafka.h"

static const char* g_topic = NULL;
static int         g_delivered = 0;
static int         g_failed = 0;

static void dr_cb(rd_kafka_t* rk, const rd_kafka_message_t* msg, void* opaque)
{
    (void)rk;
    (void)opaque;
    if (msg->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        ++g_failed;
        fprintf(stderr, "delivery failed: %s\n", rd_kafka_err2str(msg->err));
        return;
    }
    ++g_delivered;
    printf("  delivered partition %d offset %lld\n", (int)msg->partition,
           (long long)msg->offset);
}

static int produce_one(rd_kafka_t* rk, const char* what,
                       const void* key, size_t keylen,
                       const void* value, size_t valuelen,
                       rd_kafka_headers_t* headers)
{
    /* RD_KAFKA_V_VALUE / RD_KAFKA_V_KEY with a NULL pointer and length 0 is how
     * librdkafka spells "absent"; a non-NULL pointer with length 0 is "present
     * and empty". The two are different records on the wire and the whole point
     * of this program is to produce both. */
    rd_kafka_resp_err_t err = rd_kafka_producev(
        rk,
        RD_KAFKA_V_TOPIC(g_topic),
        RD_KAFKA_V_PARTITION(0),
        RD_KAFKA_V_KEY(key, keylen),
        RD_KAFKA_V_VALUE((void*)value, valuelen),
        RD_KAFKA_V_HEADERS(headers),
        RD_KAFKA_V_END);

    printf("  %-16s -> %s\n", what, rd_kafka_err2str(err));
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        /* On success librdkafka owns the headers; on failure we still do. */
        if (headers != NULL) {
            rd_kafka_headers_destroy(headers);
        }
        return 1;
    }
    return 0;
}

int main(int argc, char** argv)
{
    char  errstr[512];
    char  big[257];
    char  neighbour[512];
    const char empty[1] = { 0 };   /* a real, non-NULL, zero-length buffer */
    int   bad = 0;

    rd_kafka_conf_t*    conf = NULL;
    rd_kafka_t*         rk = NULL;
    rd_kafka_headers_t* h1 = NULL;
    rd_kafka_headers_t* h2 = NULL;
    rd_kafka_headers_t* h3 = NULL;

    if (argc != 4) {
        fprintf(stderr, "usage: %s <bootstrap> <ca.pem|-> <topic>\n", argv[0]);
        return 2;
    }
    g_topic = argv[3];

    conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", argv[1], errstr, sizeof errstr)
        != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "bootstrap.servers: %s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return 2;
    }
    if (strcmp(argv[2], "-") != 0) {
        if (rd_kafka_conf_set(conf, "security.protocol", "SSL", errstr, sizeof errstr)
                != RD_KAFKA_CONF_OK
            || rd_kafka_conf_set(conf, "ssl.ca.location", argv[2], errstr, sizeof errstr)
                != RD_KAFKA_CONF_OK
            || rd_kafka_conf_set(conf, "enable.ssl.certificate.verification", "true",
                                 errstr, sizeof errstr) != RD_KAFKA_CONF_OK) {
            fprintf(stderr, "ssl configuration: %s\n", errstr);
            rd_kafka_conf_destroy(conf);
            return 2;
        }
    }
    /* Quiet: this program's stdout is parsed by the test. */
    rd_kafka_conf_set(conf, "log_level", "0", errstr, sizeof errstr);
    rd_kafka_conf_set(conf, "message.timeout.ms", "20000", errstr, sizeof errstr);
    rd_kafka_conf_set_dr_msg_cb(conf, dr_cb);

    rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof errstr);
    if (rk == NULL) {
        fprintf(stderr, "rd_kafka_new: %s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return 2;
    }

    memset(big, 'Z', sizeof big - 1);
    big[sizeof big - 1] = '\0';
    snprintf(neighbour, sizeof neighbour, "{\"big\":\"%s\"}", big);

    /* R1 - the tombstone, with an absent, an empty and an empty-keyed header. */
    h1 = rd_kafka_headers_new(3);
    rd_kafka_header_add(h1, "\"h-null\"", 8, NULL, 0);
    rd_kafka_header_add(h1, "\"h-empty\"", 9, empty, 0);
    rd_kafka_header_add(h1, "", 0, "\"hv\"", 4);
    bad += produce_one(rk, "R1 tombstone", "\"r1-tombstone\"", 14, NULL, 0, h1);

    /* R2 - present but zero-length value. */
    h2 = rd_kafka_headers_new(1);
    rd_kafka_header_add(h2, "\"h-ok\"", 6, "123", 3);
    bad += produce_one(rk, "R2 empty value", "\"r2-emptyvalue\"", 15, empty, 0, h2);

    /* R3 - present but zero-length key, and a payload big enough that a
     * truncated document would be obvious. */
    h3 = rd_kafka_headers_new(1);
    rd_kafka_header_add(h3, "\"h-ok\"", 6, "{\"n\":1}", 7);
    bad += produce_one(rk, "R3 empty key", empty, 0, neighbour, strlen(neighbour), h3);

    /* R4 - no key at all. */
    bad += produce_one(rk, "R4 no key", NULL, 0, "{\"r4\":true}", 11, NULL);

    rd_kafka_flush(rk, 20000);
    if (rd_kafka_outq_len(rk) != 0) {
        fprintf(stderr, "%d message(s) still queued after flush\n", rd_kafka_outq_len(rk));
        ++bad;
    }
    rd_kafka_destroy(rk);

    printf("ok %d\n", g_delivered);
    return (bad == 0 && g_failed == 0 && g_delivered == 4) ? 0 : 2;
}
