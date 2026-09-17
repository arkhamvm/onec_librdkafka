// The test this whole tests/ tree exists for: does librdkafka_onec.so speak TLS
// to a Kafka 4.x broker?
//
// It is deliberately paranoid about the two ways such a test lies:
//
//   * a silent pass when nothing was actually tested. If the broker cannot be
//     reached the binary exits 77 ("skipped", the autotools convention) with an
//     explicit message, never 0.
//   * a pass that proves nothing about TLS. Several independent pieces of
//     evidence are collected, not one:
//       1. a negative control - the same client without security.protocol=SSL
//          must fail against the same bootstrap address within a short timeout,
//          which is what tells "TLS works" apart from "we hit the plaintext
//          port by accident";
//       2. a second negative control - security.protocol=SSL against an
//          unrelated CA (KAFKA_TEST_OTHER_CA) must fail, AND librdkafka's log
//          must say it failed because the certificate could not be verified,
//          so the chain is really checked and the positive result is not
//          vacuous;
//       3. the positive counterpart of both - the same probe, the same address,
//          the same timeout, but with the correct settings, has to SUCCEED.
//          Without it a small KAFKA_TEST_NEGATIVE_TIMEOUT_MS would make
//          everything fail and the two controls above would pass empty;
//       4. librdkafka's own connection log, which spells the transport out.
//          The assertions are on markers only a finished, verified handshake
//          leaves behind ("Broker SSL certificate verified",
//          "SSL_HANDSHAKE -> APIVERSION_QUERY"), on the absence of
//          "SSL handshake failed", and on no connection being (plaintext).
//
//     Both negative controls probe cluster-wide metadata, never the test topic:
//     a topic-scoped probe fails all by itself on a freshly created broker, and
//     a control that only asks "did it fail?" would pass against a plaintext
//     listener. They also assert the failure reason is not an unknown-topic
//     error, so a topic problem can never be misread as a transport problem.
//
//     The broker version is asserted too: "Kafka 4.x" is a claim, and a claim
//     nothing checks is decoration. See KAFKA_TEST_BROKER_VERSION below.
//
// On top of that it does the real work: produce N messages with distinct keys,
// values and headers, then consume them back through a fresh group and compare
// byte for byte.
//
// Everything is bounded. Every broker call carries an explicit timeout, the
// consume loop has a deadline, and a watchdog thread kills the process if the
// whole run overruns - a hung test is a broken test, and it must say which
// phase it hung in.
//
// Configuration (all optional, defaults match tests/docker):
//   KAFKA_SSL_BOOTSTRAP            localhost:9093
//   KAFKA_SSL_CA                   tests/docker/secrets/ca.pem (searched upwards)
//   KAFKA_TEST_OTHER_CA            tests/docker/secrets/other-ca.pem - a second,
//                                  unrelated CA that signs nothing the broker
//                                  uses. Written by tests/docker/gen-certs.sh.
//   KAFKA_TEST_TOPIC               onec-librdkafka-ssl-test
//   KAFKA_TEST_MESSAGES            5
//   KAFKA_TEST_PRODUCE_TIMEOUT_MS  20000   (message.timeout.ms)
//   KAFKA_TEST_CONSUME_TIMEOUT_MS  60000   (deadline for the whole poll loop)
//   KAFKA_TEST_POLL_MS             2000    (one ConsumePool batch)
//   KAFKA_TEST_ADMIN_TIMEOUT_MS    10000   (QueryWatermarkOffsets)
//   KAFKA_TEST_NEGATIVE_TIMEOUT_MS 8000    (how long a wrong config may flail;
//                                  clamped up to 5000 - below that the negative
//                                  controls would fail for reasons that have
//                                  nothing to do with the protocol)
//   KAFKA_TEST_HARD_TIMEOUT_MS     300000  (watchdog)
//   KAFKA_TEST_BROKER_VERSION      the broker version under test, e.g. 4.3.1.
//                                  Exported by tests/docker/up.sh. When it is
//                                  not set the banner says UNVERIFIED and the
//                                  run may not be quoted as evidence about any
//                                  particular Kafka generation.
//   KAFKA_TEST_EXPECT_BROKER_MAJOR 4       (the major version this run claims)
//   KAFKA_TEST_REQUIRE_BROKER      1 = an unreachable broker is a FAILURE, not
//                                  a skip. For runs that must not go quiet.
//   KAFKA_TEST_LOG                 /tmp/onec-librdkafka-ssl-<pid>.log
//   ONEC_KAFKA_SO                  path to librdkafka_onec.so (argv[1] wins)
//
// Exit codes:
//   0   every check passed
//   1   at least one check failed (or the broker was missing and
//       KAFKA_TEST_REQUIRE_BROKER=1)
//   2   setup problem - the component could not be loaded
//   3   hard timeout, the run was killed by the watchdog
//   77  skipped - the broker is not reachable / the CA is missing

#include "host/component_loader.h"
#include "host/test_assert.h"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {

using onec::Arg;
using onec::ComponentLibrary;
using onec::ComponentObject;
using onec::HostError;
using onec::Value;

using Clock = std::chrono::steady_clock;

// src/component_types.h:31-33 - what ConsumePool returns.
const long kConsumerFatalError = -1;
const long kConsumerError      = 0;
const long kConsumerNoError    = 1;

// src/errors.cpp:33 - ErrorDescription on success. Not an empty string.
const char* const kNoError = "Sucess";

const int kExitOk      = 0;
const int kExitFailed  = 1;
const int kExitSetup   = 2;
const int kExitTimeout = 3;
const int kExitSkipped = 77;

//---------------------------------------------------------------------------//
// Progress
//---------------------------------------------------------------------------//

// The current phase is shared with the watchdog thread, so a hard timeout can
// say what the process was doing when it stopped making progress.
std::mutex                  g_phaseMutex;
std::string                 g_phase = "startup";
const Clock::time_point     g_started = Clock::now();

double elapsedSeconds()
{
    return std::chrono::duration<double>(Clock::now() - g_started).count();
}

std::string stamp()
{
    std::ostringstream out;
    out << "t+" << std::fixed << std::setprecision(1) << std::setw(6) << elapsedSeconds() << "s";
    return out.str();
}

void phase(const std::string& what)
{
    {
        std::lock_guard<std::mutex> lock(g_phaseMutex);
        g_phase = what;
    }
    std::cout << "  ...   [" << stamp() << "] " << what << std::endl;
}

std::string currentPhase()
{
    std::lock_guard<std::mutex> lock(g_phaseMutex);
    return g_phase;
}

// Long strings (JSON blobs, log excerpts) go into failure messages; keep them
// readable.
std::string clip(const std::string& text, std::size_t limit = 600)
{
    if (text.size() <= limit) {
        return text;
    }
    return text.substr(0, limit) + "... (" + std::to_string(text.size()) + " bytes total)";
}

//---------------------------------------------------------------------------//
// Watchdog
//---------------------------------------------------------------------------//

// Kills the process if the run overruns. Nothing here is recoverable: by the
// time it fires the main thread is stuck inside librdkafka, so it prints and
// calls _Exit rather than trying to unwind.
class Watchdog
{
public:
    explicit Watchdog(std::chrono::milliseconds limit)
        : limit_(limit)
    {
        thread_ = std::thread([this] { this->wait(); });
    }

    ~Watchdog()
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            done_ = true;
        }
        cv_.notify_all();
        thread_.join();
    }

    Watchdog(const Watchdog&) = delete;
    Watchdog& operator=(const Watchdog&) = delete;

private:
    void wait()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        if (cv_.wait_for(lock, limit_, [this] { return done_; })) {
            return;
        }
        lock.unlock();

        std::cout.flush();
        std::cerr << "\n[ KILL ] hard timeout after " << limit_.count()
                  << " ms, stuck in phase: " << currentPhase() << std::endl;
        std::cerr << "         the component or the broker stopped responding; "
                     "raise KAFKA_TEST_HARD_TIMEOUT_MS only if the box is genuinely slow"
                  << std::endl;
        std::cerr.flush();
        std::_Exit(kExitTimeout);
    }

    std::chrono::milliseconds limit_;
    std::mutex                mutex_;
    std::condition_variable   cv_;
    bool                      done_ = false;
    std::thread               thread_;
};

//---------------------------------------------------------------------------//
// A JSON reader, just enough for what the component emits
//---------------------------------------------------------------------------//

// The component hands back JSON as text (GetJSONDeliveryReport,
// ReceiveJSONMessages, QueryWatermarkOffsets) and the test has to check the
// values inside it. nlohmann/json lives under src/, which this test is not
// allowed to depend on, so here is a small reader instead. It is strict:
// anything malformed throws, which is itself a useful assertion.
namespace mini {

enum class Kind { Null, Bool, Number, String, Array, Object };

struct Node;
using Nodes   = std::vector<Node>;
using Members = std::vector<std::pair<std::string, Node>>;

struct Node
{
    Kind        kind = Kind::Null;
    bool        boolean = false;
    double      number = 0.0;
    std::string text;
    Nodes       items;
    Members     members;

    bool isNull() const { return kind == Kind::Null; }
    bool isBool() const { return kind == Kind::Bool; }
    bool isNumber() const { return kind == Kind::Number; }
    bool isString() const { return kind == Kind::String; }
    bool isArray() const { return kind == Kind::Array; }
    bool isObject() const { return kind == Kind::Object; }

    const Node* member(const std::string& key) const
    {
        for (const auto& entry : members) {
            if (entry.first == key) {
                return &entry.second;
            }
        }
        return nullptr;
    }

    bool has(const std::string& key) const { return member(key) != nullptr; }

    // Missing or wrong-typed members read as the fallback; the caller asserts
    // on the value, so a missing field shows up as a value mismatch with a
    // readable message rather than as an exception.
    std::string str(const std::string& key, const std::string& fallback = std::string()) const
    {
        const Node* node = member(key);
        return (node != nullptr && node->isString()) ? node->text : fallback;
    }

    double num(const std::string& key, double fallback = -1.0) const
    {
        const Node* node = member(key);
        return (node != nullptr && node->isNumber()) ? node->number : fallback;
    }

    const Nodes& array(const std::string& key) const
    {
        static const Nodes empty;
        const Node* node = member(key);
        return (node != nullptr && node->isArray()) ? node->items : empty;
    }
};

class Parser
{
public:
    explicit Parser(const std::string& text) : text_(text) {}

    Node parse()
    {
        skipSpace();
        Node root = parseValue(0);
        skipSpace();
        if (pos_ != text_.size()) {
            fail("trailing characters after the top-level value");
        }
        return root;
    }

private:
    static const int kMaxDepth = 32;

    [[noreturn]] void fail(const std::string& what) const
    {
        throw std::runtime_error("invalid JSON at offset " + std::to_string(pos_) + ": " + what);
    }

    void skipSpace()
    {
        while (pos_ < text_.size()) {
            const char ch = text_[pos_];
            if (ch != ' ' && ch != '\t' && ch != '\n' && ch != '\r') {
                break;
            }
            ++pos_;
        }
    }

    char cur() const { return pos_ < text_.size() ? text_[pos_] : '\0'; }

    void expect(char ch)
    {
        if (cur() != ch) {
            fail(std::string("expected '") + ch + "'");
        }
        ++pos_;
    }

    bool tryLiteral(const char* word)
    {
        const std::size_t len = std::strlen(word);
        if (text_.compare(pos_, len, word) != 0) {
            return false;
        }
        pos_ += len;
        return true;
    }

    static void appendUtf8(std::string& out, unsigned int cp)
    {
        if (cp < 0x80) {
            out.push_back(static_cast<char>(cp));
        } else if (cp < 0x800) {
            out.push_back(static_cast<char>(0xC0 | (cp >> 6)));
            out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
        } else if (cp < 0x10000) {
            out.push_back(static_cast<char>(0xE0 | (cp >> 12)));
            out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
            out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
        } else {
            out.push_back(static_cast<char>(0xF0 | (cp >> 18)));
            out.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
            out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
            out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
        }
    }

    unsigned int parseHex4()
    {
        if (pos_ + 4 > text_.size()) {
            fail("truncated \\u escape");
        }
        unsigned int value = 0;
        for (int i = 0; i < 4; ++i) {
            const char ch = text_[pos_++];
            value <<= 4;
            if (ch >= '0' && ch <= '9') {
                value |= static_cast<unsigned int>(ch - '0');
            } else if (ch >= 'a' && ch <= 'f') {
                value |= static_cast<unsigned int>(ch - 'a' + 10);
            } else if (ch >= 'A' && ch <= 'F') {
                value |= static_cast<unsigned int>(ch - 'A' + 10);
            } else {
                fail("bad hex digit in \\u escape");
            }
        }
        return value;
    }

    std::string parseString()
    {
        expect('"');
        std::string out;
        for (;;) {
            if (pos_ >= text_.size()) {
                fail("unterminated string");
            }
            const char ch = text_[pos_++];
            if (ch == '"') {
                return out;
            }
            if (ch != '\\') {
                out.push_back(ch);
                continue;
            }
            if (pos_ >= text_.size()) {
                fail("unterminated escape sequence");
            }
            const char esc = text_[pos_++];
            switch (esc) {
            case '"':  out.push_back('"');  break;
            case '\\': out.push_back('\\'); break;
            case '/':  out.push_back('/');  break;
            case 'b':  out.push_back('\b'); break;
            case 'f':  out.push_back('\f'); break;
            case 'n':  out.push_back('\n'); break;
            case 'r':  out.push_back('\r'); break;
            case 't':  out.push_back('\t'); break;
            case 'u': {
                unsigned int cp = parseHex4();
                if (cp >= 0xD800 && cp <= 0xDBFF && text_.compare(pos_, 2, "\\u") == 0) {
                    const std::size_t saved = pos_;
                    pos_ += 2;
                    const unsigned int low = parseHex4();
                    if (low >= 0xDC00 && low <= 0xDFFF) {
                        cp = 0x10000u + ((cp - 0xD800u) << 10) + (low - 0xDC00u);
                    } else {
                        pos_ = saved;
                    }
                }
                appendUtf8(out, cp);
                break;
            }
            default:
                fail("unknown escape sequence");
            }
        }
    }

    Node parseNumber()
    {
        const std::size_t start = pos_;
        if (cur() == '-' || cur() == '+') {
            ++pos_;
        }
        while (pos_ < text_.size()) {
            const char ch = text_[pos_];
            const bool numeric = (std::isdigit(static_cast<unsigned char>(ch)) != 0) || ch == '.' ||
                                 ch == 'e' || ch == 'E' || ch == '+' || ch == '-';
            if (!numeric) {
                break;
            }
            ++pos_;
        }
        if (pos_ == start) {
            fail("expected a value");
        }
        Node node;
        node.kind = Kind::Number;
        try {
            node.number = std::stod(text_.substr(start, pos_ - start));
        } catch (const std::exception&) {
            fail("malformed number");
        }
        return node;
    }

    Node parseValue(int depth)
    {
        if (depth > kMaxDepth) {
            fail("nesting too deep");
        }
        skipSpace();

        Node node;
        if (cur() == '{') {
            ++pos_;
            node.kind = Kind::Object;
            skipSpace();
            if (cur() == '}') {
                ++pos_;
                return node;
            }
            for (;;) {
                skipSpace();
                std::string key = parseString();
                skipSpace();
                expect(':');
                Node value = parseValue(depth + 1);
                node.members.emplace_back(std::move(key), std::move(value));
                skipSpace();
                if (cur() == ',') {
                    ++pos_;
                    continue;
                }
                expect('}');
                return node;
            }
        }
        if (cur() == '[') {
            ++pos_;
            node.kind = Kind::Array;
            skipSpace();
            if (cur() == ']') {
                ++pos_;
                return node;
            }
            for (;;) {
                node.items.push_back(parseValue(depth + 1));
                skipSpace();
                if (cur() == ',') {
                    ++pos_;
                    continue;
                }
                expect(']');
                return node;
            }
        }
        if (cur() == '"') {
            node.kind = Kind::String;
            node.text = parseString();
            return node;
        }
        if (tryLiteral("true")) {
            node.kind = Kind::Bool;
            node.boolean = true;
            return node;
        }
        if (tryLiteral("false")) {
            node.kind = Kind::Bool;
            node.boolean = false;
            return node;
        }
        if (tryLiteral("null")) {
            node.kind = Kind::Null;
            return node;
        }
        return parseNumber();
    }

    const std::string& text_;
    std::size_t        pos_ = 0;
};

Node parse(const std::string& text)
{
    Parser parser(text);
    return parser.parse();
}

// Escapes a string for the JSON the component parses in SetJSONMessageList.
std::string quoted(const std::string& raw)
{
    std::string out;
    out.reserve(raw.size() + 2);
    out.push_back('"');
    for (const char signedCh : raw) {
        const unsigned char ch = static_cast<unsigned char>(signedCh);
        switch (ch) {
        case '"':  out += "\\\""; break;
        case '\\': out += "\\\\"; break;
        case '\b': out += "\\b";  break;
        case '\f': out += "\\f";  break;
        case '\n': out += "\\n";  break;
        case '\r': out += "\\r";  break;
        case '\t': out += "\\t";  break;
        default:
            if (ch < 0x20) {
                char buffer[8];
                std::snprintf(buffer, sizeof(buffer), "\\u%04x", static_cast<unsigned int>(ch));
                out += buffer;
            } else {
                out.push_back(signedCh);
            }
        }
    }
    out.push_back('"');
    return out;
}

} // namespace mini

//---------------------------------------------------------------------------//
// Small filesystem / environment helpers
//---------------------------------------------------------------------------//

bool fileExists(const std::string& path)
{
    std::ifstream file(path, std::ios::binary);
    return file.good();
}

std::string readFile(const std::string& path)
{
    std::ifstream file(path, std::ios::binary);
    if (!file) {
        return std::string();
    }
    std::ostringstream buffer;
    buffer << file.rdbuf();
    return buffer.str();
}

std::string envOr(const char* name, const std::string& fallback)
{
    const char* value = std::getenv(name);
    return (value != nullptr && value[0] != '\0') ? std::string(value) : fallback;
}

int envInt(const char* name, int fallback)
{
    const char* value = std::getenv(name);
    if (value == nullptr || value[0] == '\0') {
        return fallback;
    }
    try {
        const int parsed = std::stoi(value);
        if (parsed > 0) {
            return parsed;
        }
    } catch (const std::exception&) {
        // fall through
    }
    std::cerr << "warning: " << name << "='" << value
              << "' is not a positive integer, using " << fallback << std::endl;
    return fallback;
}

// The same, with a floor. Some of these timeouts are not merely "how long we
// are willing to wait": below a certain value the thing being measured cannot
// happen at all. KAFKA_TEST_NEGATIVE_TIMEOUT_MS=1 used to make both negative
// controls fail instantly - for reasons that have nothing to do with the
// protocol - while both CHECKs passed and the suite stayed green with the two
// witnesses hollowed out. A configuration knob may not be able to do that.
int envIntAtLeast(const char* name, int fallback, int floorValue)
{
    const int value = envInt(name, fallback);
    if (value < floorValue) {
        std::cerr << "warning: " << name << "=" << value << " is below the " << floorValue
                  << " this test needs to tell a protocol failure from a plain timeout; using "
                  << floorValue << std::endl;
        return floorValue;
    }
    return value;
}

std::size_t countOccurrences(const std::string& haystack, const std::string& needle)
{
    if (needle.empty()) {
        return 0;
    }
    std::size_t count = 0;
    std::size_t at = haystack.find(needle);
    while (at != std::string::npos) {
        ++count;
        at = haystack.find(needle, at + needle.size());
    }
    return count;
}

// Up to `limit` whole lines of `text` that contain `needle`.
std::vector<std::string> linesContaining(const std::string& text, const std::string& needle,
                                         std::size_t limit)
{
    std::vector<std::string> found;
    std::istringstream stream(text);
    std::string line;
    while (found.size() < limit && std::getline(stream, line)) {
        if (line.find(needle) != std::string::npos) {
            found.push_back(line);
        }
    }
    return found;
}

std::string lowered(std::string text)
{
    std::transform(text.begin(), text.end(), text.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return text;
}

bool containsNoCase(const std::string& haystack, const std::string& needle)
{
    return lowered(haystack).find(lowered(needle)) != std::string::npos;
}

// "the topic is not there" is not "the transport is broken", and a negative
// control that accepts the first as proof of the second proves nothing.
// tests/docker/down.sh removes the volumes, so every run starts against a
// cluster with no topics at all: a topic-scoped probe then fails with
// UNKNOWN_TOPIC_OR_PART over a perfectly healthy plaintext connection. Every
// control below asserts its failure was NOT one of these.
bool looksLikeUnknownTopic(const std::string& error)
{
    return containsNoCase(error, "unknown topic") || containsNoCase(error, "unknown partition") ||
           containsNoCase(error, "UNKNOWN_TOPIC_OR_PART");
}

// What OpenSSL and librdkafka say when the peer certificate does not chain to
// the configured CA. Several spellings: the exact text depends on the OpenSSL
// version and on which layer reports first.
bool mentionsCertificateRejection(const std::string& log)
{
    return containsNoCase(log, "certificate verify failed") ||
           containsNoCase(log, "unable to get local issuer certificate") ||
           containsNoCase(log, "self signed certificate") ||
           containsNoCase(log, "self-signed certificate") ||
           containsNoCase(log, "broker certificate could not be verified");
}

// "4.3.1" -> "4", "apache/kafka:4.3.1" -> "4", "v3.9.0" -> "3", garbage -> "".
std::string majorVersion(const std::string& version)
{
    std::size_t at = version.find_last_of(':');
    at = (at == std::string::npos) ? 0 : at + 1;
    while (at < version.size() && (version[at] == 'v' || version[at] == 'V' ||
                                   std::isspace(static_cast<unsigned char>(version[at])) != 0)) {
        ++at;
    }
    std::string digits;
    while (at < version.size() && std::isdigit(static_cast<unsigned char>(version[at])) != 0) {
        digits += version[at];
        ++at;
    }
    return digits;
}

//---------------------------------------------------------------------------//
// Waiting for librdkafka's log to catch up
//---------------------------------------------------------------------------//

struct LogWait
{
    bool        found = false;   // the marker is on disk
    std::string text;            // whatever the file held on the last read
    long long   waitedMs = 0;
};

// MyEventCb (src/consumer1c_event_cb.cpp:36-86) opens the log file, appends one
// event and closes it again - once per event, from librdkafka's own threads.
// Reading the file after a fixed sleep is therefore a race, and a race with a
// nasty failure mode: the missing tail reads exactly like good news ("no
// plaintext connection, no failed handshake"), so a loaded box turns a
// truncated file into a pass. Poll instead, until the marker that says the
// interesting part has been written actually appears, and let the caller fail
// on the deadline rather than conclude anything from half a file.
LogWait waitForLogMarker(const std::string& path, const std::string& marker, int deadlineMs)
{
    LogWait                 result;
    const Clock::time_point begun = Clock::now();
    const Clock::time_point deadline = begun + std::chrono::milliseconds(deadlineMs);
    for (;;) {
        result.text = readFile(path);
        if (result.text.find(marker) != std::string::npos) {
            result.found = true;
            break;
        }
        if (Clock::now() >= deadline) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    result.waitedMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - begun).count();
    return result;
}

// How long the log file may lag behind the call that produced the events.
const int kLogWaitMs = 5000;

// What librdkafka writes when a TLS handshake actually completes, and when it
// does not. These are the only strings in the log that separate the two: see
// caseTransportEvidence for the measured counts and for what the obvious
// candidates match instead.
const char* const kMarkerCertVerified    = "Broker SSL certificate verified";
const char* const kMarkerHandshakeDone   = "SSL_HANDSHAKE -> APIVERSION_QUERY";
const char* const kMarkerHandshakeFailed = "SSL handshake failed";

//---------------------------------------------------------------------------//
// Is the broker even there?
//---------------------------------------------------------------------------//

// A plain TCP connect to the first address in the bootstrap list. It says
// nothing about TLS - that is the rest of the file's job - it only decides
// between "run the test" and "skip loudly".
bool tcpReachable(const std::string& bootstrap, int timeoutMs, std::string* detail)
{
    std::string host = bootstrap;
    const std::size_t comma = host.find(',');
    if (comma != std::string::npos) {
        host.erase(comma);
    }
    std::string port = "9093";
    const std::size_t colon = host.rfind(':');
    if (colon != std::string::npos) {
        port = host.substr(colon + 1);
        host.erase(colon);
    }
    if (host.empty()) {
        host = "localhost";
    }

    addrinfo hints;
    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    addrinfo* list = nullptr;
    const int rc = ::getaddrinfo(host.c_str(), port.c_str(), &hints, &list);
    if (rc != 0) {
        *detail = "getaddrinfo(" + host + ":" + port + "): " + ::gai_strerror(rc);
        return false;
    }

    bool        ok = false;
    std::string last = "no usable address";
    for (addrinfo* ai = list; ai != nullptr && !ok; ai = ai->ai_next) {
        const int fd = ::socket(ai->ai_family, ai->ai_socktype | SOCK_NONBLOCK, ai->ai_protocol);
        if (fd < 0) {
            last = std::string("socket(): ") + std::strerror(errno);
            continue;
        }
        if (::connect(fd, ai->ai_addr, ai->ai_addrlen) == 0) {
            ok = true;
            ::close(fd);
            break;
        }
        if (errno != EINPROGRESS) {
            last = std::string("connect(): ") + std::strerror(errno);
            ::close(fd);
            continue;
        }
        pollfd pfd;
        std::memset(&pfd, 0, sizeof(pfd));
        pfd.fd = fd;
        pfd.events = POLLOUT;
        const int ready = ::poll(&pfd, 1, timeoutMs);
        if (ready == 0) {
            last = "timed out after " + std::to_string(timeoutMs) + " ms";
        } else if (ready < 0) {
            last = std::string("poll(): ") + std::strerror(errno);
        } else {
            int       error = 0;
            socklen_t length = sizeof(error);
            if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &length) == 0 && error == 0) {
                ok = true;
            } else {
                last = std::string("connect(): ") + std::strerror(error != 0 ? error : errno);
            }
        }
        ::close(fd);
    }
    ::freeaddrinfo(list);

    if (!ok) {
        *detail = host + ":" + port + " - " + last;
    }
    return ok;
}

//---------------------------------------------------------------------------//
// Configuration
//---------------------------------------------------------------------------//

struct Config
{
    std::string bootstrap;
    std::string caPath;
    std::string otherCaPath;          // KAFKA_TEST_OTHER_CA - the unrelated CA
    std::string brokerVersion;        // KAFKA_TEST_BROKER_VERSION, may be empty
    std::string expectBrokerMajor;    // KAFKA_TEST_EXPECT_BROKER_MAJOR
    bool        requireBroker = false;// KAFKA_TEST_REQUIRE_BROKER=1
    std::string topic;
    std::string groupPrefix;
    std::string clientPrefix;
    std::string runTag;
    std::string logPath;
    std::string soPath;

    int messageCount      = 5;
    int produceTimeoutMs  = 20000;
    int consumeTimeoutMs  = 60000;
    int pollMs            = 2000;
    int adminTimeoutMs    = 10000;
    int negativeTimeoutMs = 8000;
    int hardTimeoutMs     = 300000;
    int probeTimeoutMs    = 3000;

    std::string group(const std::string& suffix) const { return groupPrefix + "-" + suffix; }
    std::string client(const std::string& suffix) const { return clientPrefix + "-" + suffix; }
};

// The same search the component loader does for the .so, applied to the CA the
// docker stage writes, so the binary works from the repo root or from tests/.
std::string defaultCaPath()
{
    const char* const candidates[] = {
        "tests/docker/secrets/ca.pem",
        "docker/secrets/ca.pem",
        "../tests/docker/secrets/ca.pem",
        "../../tests/docker/secrets/ca.pem",
        "secrets/ca.pem",
    };
    for (const char* candidate : candidates) {
        if (fileExists(candidate)) {
            return candidate;
        }
    }
    return "tests/docker/secrets/ca.pem";
}

// A CA that is definitely not the broker's, for the "is the chain actually
// verified?" control. tests/docker/gen-certs.sh writes it next to ca.pem and
// checks that it signs nothing the broker uses.
//
// This used to be the system bundle (/etc/ssl/certs/...), which is wrong twice
// over: it is not there on every box, and when it was missing the control was
// dropped with a note and a return - no failure, no skip, a green run with the
// witness quietly absent. A control that can be skipped is not a control.
std::string defaultOtherCaPath()
{
    const char* const candidates[] = {
        "tests/docker/secrets/other-ca.pem",
        "docker/secrets/other-ca.pem",
        "../tests/docker/secrets/other-ca.pem",
        "../../tests/docker/secrets/other-ca.pem",
        "secrets/other-ca.pem",
    };
    for (const char* candidate : candidates) {
        if (fileExists(candidate)) {
            return candidate;
        }
    }
    return "tests/docker/secrets/other-ca.pem";
}

Config buildConfig(int argc, char** argv)
{
    Config cfg;
    cfg.bootstrap   = envOr("KAFKA_SSL_BOOTSTRAP", "localhost:9093");
    cfg.caPath      = envOr("KAFKA_SSL_CA", defaultCaPath());
    cfg.otherCaPath = envOr("KAFKA_TEST_OTHER_CA", defaultOtherCaPath());
    cfg.topic       = envOr("KAFKA_TEST_TOPIC", "onec-librdkafka-ssl-test");

    // Deliberately no default for the version itself: an unset variable means
    // "nobody told us", which is not the same as "it is 4.x", and the banner
    // says so. Only the expectation has a default.
    cfg.brokerVersion     = envOr("KAFKA_TEST_BROKER_VERSION", std::string());
    cfg.expectBrokerMajor = envOr("KAFKA_TEST_EXPECT_BROKER_MAJOR", "4");
    cfg.requireBroker     = envOr("KAFKA_TEST_REQUIRE_BROKER", "0") == "1";

    cfg.messageCount      = envInt("KAFKA_TEST_MESSAGES", 5);
    cfg.produceTimeoutMs  = envInt("KAFKA_TEST_PRODUCE_TIMEOUT_MS", 20000);
    cfg.consumeTimeoutMs  = envInt("KAFKA_TEST_CONSUME_TIMEOUT_MS", 60000);
    cfg.pollMs            = envInt("KAFKA_TEST_POLL_MS", 2000);
    cfg.adminTimeoutMs    = envInt("KAFKA_TEST_ADMIN_TIMEOUT_MS", 10000);
    // Floored, not just defaulted - see envIntAtLeast.
    cfg.negativeTimeoutMs = envIntAtLeast("KAFKA_TEST_NEGATIVE_TIMEOUT_MS", 8000, 5000);
    cfg.hardTimeoutMs     = envInt("KAFKA_TEST_HARD_TIMEOUT_MS", 300000);

    // Unique per run: the topic is reused across runs, so the consumer has to be
    // able to tell this run's messages from the leftovers, and a stale consumer
    // group must not hand us a committed offset past our own messages.
    const long long pid = static_cast<long long>(::getpid());
    const long long tick =
        std::chrono::duration_cast<std::chrono::microseconds>(Clock::now().time_since_epoch()).count();
    std::ostringstream tag;
    tag << "ssl" << pid << "x" << (tick % 1000000000LL);
    cfg.runTag = tag.str();

    cfg.groupPrefix  = "onec-librdkafka-" + cfg.runTag;
    cfg.clientPrefix = "onec-librdkafka-" + cfg.runTag;
    cfg.logPath = envOr("KAFKA_TEST_LOG", "/tmp/onec-librdkafka-ssl-" + std::to_string(pid) + ".log");

    cfg.soPath = (argc > 1 && argv[1] != nullptr && argv[1][0] != '\0')
                     ? std::string(argv[1])
                     : ComponentLibrary::defaultPath();
    return cfg;
}

Config g_cfg;

//---------------------------------------------------------------------------//
// The messages under test
//---------------------------------------------------------------------------//

struct Header
{
    std::string key;
    std::string value;
};

struct TestMessage
{
    std::string         key;
    std::string         value;
    std::vector<Header> headers;
};

std::vector<TestMessage> g_expected;     // what the producer sent
bool                     g_produced = false;

std::vector<TestMessage> buildMessages(const Config& cfg)
{
    std::vector<TestMessage> messages;
    messages.reserve(static_cast<std::size_t>(cfg.messageCount));

    for (int i = 0; i < cfg.messageCount; ++i) {
        std::ostringstream index;
        index << std::setw(3) << std::setfill('0') << i;

        TestMessage message;
        message.key = cfg.runTag + "-key-" + index.str();
        // Non-ASCII on purpose: the payload crosses UTF-16 at the 1C boundary on
        // the way in and comes back as UTF-8 inside a JSON string, so a broken
        // conversion shows up as a mismatch rather than as silence.
        //
        // Cyrillic (2 bytes per code unit) rather than CJK, and this is now a
        // free choice rather than a constraint. It used to be a constraint:
        // ComponentBase::toUTF8String sized its output buffer at 2 bytes per
        // UTF-16 code unit, so any character in U+0800..U+FFFF overflowed it and
        // a 3-byte character here would have been an unmarked tripwire making an
        // unrelated case fail for a reason nobody would guess. The buffer is now
        // sized at the true bound and 3- and 4-byte characters are covered
        // deliberately, by caseThreeByteUtf8 below.
        message.value = "message " + index.str() + " of run " + cfg.runTag +
                        " / \xD0\xBF\xD1\x80\xD0\xBE\xD0\xB2\xD0\xB5\xD1\x80\xD0\xBA\xD0\xB0 TLS";
        message.headers.push_back(Header{"onec-test-run", cfg.runTag});
        message.headers.push_back(Header{"onec-test-index", index.str()});
        messages.push_back(std::move(message));
    }
    return messages;
}

// The format SetJSONMessageList expects (README.md, "API отправителя"):
//   [{"Key": "...", "Value": "...", "Headers": [{"HeaderName": "HeaderValue"}]}]
// Note that each Headers element is an object keyed by the header name - it is
// NOT the {"Key": ..., "Value": ...} shape the consumer hands back.
std::string buildProduceJson(const std::vector<TestMessage>& messages)
{
    std::string json = "[";
    for (std::size_t i = 0; i < messages.size(); ++i) {
        if (i != 0) {
            json += ",";
        }
        json += "{\"Key\":" + mini::quoted(messages[i].key);
        json += ",\"Value\":" + mini::quoted(messages[i].value);
        json += ",\"Headers\":[";
        for (std::size_t h = 0; h < messages[i].headers.size(); ++h) {
            if (h != 0) {
                json += ",";
            }
            json += "{" + mini::quoted(messages[i].headers[h].key) + ":" +
                    mini::quoted(messages[i].headers[h].value) + "}";
        }
        json += "]}";
    }
    json += "]";
    return json;
}

std::string describeHeaders(const std::vector<Header>& headers)
{
    std::string text = "[";
    for (std::size_t i = 0; i < headers.size(); ++i) {
        if (i != 0) {
            text += ", ";
        }
        text += headers[i].key + "=" + headers[i].value;
    }
    text += "]";
    return text;
}

//---------------------------------------------------------------------------//
// Shared SSL wiring
//---------------------------------------------------------------------------//

struct ConfEntry
{
    std::string key;
    std::string value;
};

// Everything a client needs to speak TLS to the test broker, as data, so the
// same configuration can be handed to a producer, a consumer or an admin
// client - and so a negative control can build the same thing with one value
// changed and nothing else.
std::vector<ConfEntry> sslConfEntries(const std::string& caPath, const std::string& clientId)
{
    return std::vector<ConfEntry>{
        {"security.protocol", "SSL"},
        {"ssl.ca.location", caPath},
        // Explicit rather than implicit: if this ever defaulted to off, a broken
        // chain would still "work" and the whole test would be worthless.
        {"enable.ssl.certificate.verification", "true"},
        {"client.id", clientId},
    };
}

// Must run before Initialize: the component applies the accumulated conf when
// it creates the underlying librdkafka handle.
void applyConf(ComponentObject& object, const std::vector<ConfEntry>& conf)
{
    for (const ConfEntry& entry : conf) {
        REQUIRE_MSG(object.callBool(u"SetGlobalConf", {entry.key, entry.value}),
                    "SetGlobalConf(" + entry.key + ", " + entry.value +
                        ") failed: " + object.errorDescription());
    }
}

void applySslConf(ComponentObject& object, const std::string& clientId)
{
    applyConf(object, sslConfEntries(g_cfg.caPath, clientId));
}

void checkNoLeaks(ComponentObject& object, const char* what)
{
    CHECK_MSG(object.memory().liveBlocks() == 0,
              std::string(what) + ": " + std::to_string(object.memory().liveBlocks()) +
                  " memory manager block(s) still allocated");
}

//---------------------------------------------------------------------------//
// A probe that does not depend on the topic
//---------------------------------------------------------------------------//

struct ProbeResult
{
    bool        ok = false;      // GetMetadata came back with a JSON string
    std::string error;           // ErrorDescription right after the call
    std::string detail;          // the metadata, clipped, or the empty variant
    long long   spentMs = 0;
};

// One cluster-wide metadata call through KafkaAdminClient.
//
// Topic-independent on purpose, and that is the whole point of this helper.
// The obvious probe - QueryWatermarkOffsets on the test topic - is useless as a
// transport control: tests/docker/down.sh removes the volumes, so every run
// meets a cluster with no topics, the call fails with "Local: Unknown
// partition" over a perfectly healthy connection, and a control that only asks
// "did it fail?" goes green against a plaintext listener. (Measured: pointing
// KAFKA_SSL_BOOTSTRAP at :9092 made the old control print "plaintext attempt
// failed in 5 ms: Unhandled: Local: Unknown partition" and then pass.)
//
// GetMetadata(timeout, EMPTY) takes the VTYPE_EMPTY branch of
// src/admin_client1c.cpp:307-336, which leaves `topic` empty, and
// src/admin_client1c_core.cpp:551-579 then calls
// rd_kafka_metadata(rk, all_topics=1, nullptr, ...). No topic is named
// anywhere, an empty cluster is a perfectly good answer, and the only thing
// left to fail on is reaching the broker.
ProbeResult probeClusterMetadata(ComponentLibrary& lib, const std::vector<ConfEntry>& conf,
                                 int timeoutMs, const std::string& what)
{
    ProbeResult     result;
    ComponentObject admin(lib, u"KafkaAdminClient");
    applyConf(admin, conf);
    REQUIRE_MSG(admin.callBool(u"Initialize", {g_cfg.bootstrap}),
                what + ": admin Initialize failed: " + admin.errorDescription());

    const Clock::time_point begun = Clock::now();
    {
        // Scoped: the Value owns a block the component allocated through the
        // memory manager and hands it back on destruction. checkNoLeaks below
        // would otherwise report that perfectly legitimate block as a leak -
        // and it would do so exactly when the probe SUCCEEDS, i.e. when a
        // negative control is busy reporting a real problem.
        Value metadata = admin.callFunc(u"GetMetadata", {timeoutMs, Arg::empty()});
        result.spentMs =
            std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - begun).count();
        result.ok = metadata.isString();
        result.detail = result.ok ? clip(metadata.asString(), 200) : metadata.describe();
    }
    result.error = admin.errorDescription();
    checkNoLeaks(admin, what.c_str());
    return result;
}

//---------------------------------------------------------------------------//
// Case 1 - negative controls
//---------------------------------------------------------------------------//

// Without these, "the producer delivered messages" would only prove that some
// broker answered on some port. They say the port is TLS-only and that the
// certificate chain is genuinely checked - and control C, at the end, says the
// timeout all of them ran under is long enough for any of that to mean
// anything.
void caseNegativeControls(ComponentLibrary& lib)
{
    //-----------------------------------------------------------------------//
    // (A) The bootstrap port must not answer plaintext.
    //-----------------------------------------------------------------------//
    phase("negative control A: plaintext client against " + g_cfg.bootstrap);
    const std::vector<ConfEntry> plaintextConf = {
        // Deliberately no security.protocol: librdkafka defaults to plaintext.
        {"client.id", g_cfg.client("plaintext")},
        {"socket.timeout.ms", "4000"},
    };
    const ProbeResult plain =
        probeClusterMetadata(lib, plaintextConf, g_cfg.negativeTimeoutMs, "plaintext admin client");
    test::note("plaintext attempt finished in " + std::to_string(plain.spentMs) + " ms: " +
               plain.error);

    CHECK_MSG(!plain.ok,
              "a PLAINTEXT client reached " + g_cfg.bootstrap + " and read cluster metadata (" +
                  plain.detail +
                  "). That port is not TLS-only, so nothing below proves the component did TLS - "
                  "check KAFKA_SSL_BOOTSTRAP really points at the SSL listener");
    CHECK_MSG(plain.error != kNoError,
              "plaintext against the TLS port reported success: ErrorDescription='" + plain.error +
                  "'");
    CHECK_MSG(!looksLikeUnknownTopic(plain.error),
              "the plaintext probe failed with a topic error rather than a transport error ('" +
                  plain.error +
                  "'). A missing topic says nothing about the protocol, so this control is void "
                  "until the probe fails on the connection itself");
    CHECK_MSG(plain.spentMs <= g_cfg.negativeTimeoutMs + 5000,
              "the plaintext attempt took " + std::to_string(plain.spentMs) + " ms, more than the " +
                  std::to_string(g_cfg.negativeTimeoutMs) + " ms timeout it was given");

    //-----------------------------------------------------------------------//
    // (B) The certificate chain must really be verified.
    //-----------------------------------------------------------------------//
    phase("negative control B: TLS with an unrelated CA (" + g_cfg.otherCaPath + ")");
    REQUIRE_MSG(fileExists(g_cfg.otherCaPath),
                "the unrelated CA '" + g_cfg.otherCaPath +
                    "' does not exist. tests/docker/gen-certs.sh writes it next to ca.pem; set "
                    "KAFKA_TEST_OTHER_CA if it lives elsewhere. This control is not optional - "
                    "without it nothing in this file shows the certificate chain is checked at "
                    "all - so a missing CA fails the run instead of quietly skipping it");

    const ProbeResult wrongCa =
        probeClusterMetadata(lib, sslConfEntries(g_cfg.otherCaPath, g_cfg.client("wrongca")),
                             g_cfg.negativeTimeoutMs, "wrong-CA admin client");
    test::note("wrong-CA attempt finished in " + std::to_string(wrongCa.spentMs) + " ms: " +
               wrongCa.error);

    CHECK_MSG(!wrongCa.ok,
              "TLS against an unrelated CA succeeded (" + wrongCa.detail +
                  "), so the broker certificate is not being verified and a successful handshake "
                  "below would prove nothing about trust");
    CHECK_MSG(!looksLikeUnknownTopic(wrongCa.error),
              "the wrong-CA probe failed with a topic error rather than a certificate error ('" +
                  wrongCa.error + "'), so it says nothing about trust");

    // ...and WHY it failed. "It failed" on its own is equally what a typo in the
    // bootstrap address produces. Only librdkafka's own log says the chain was
    // checked and rejected, and KafkaConsumer is the only class that registers
    // an event_cb, so the reason has to be collected through one.
    phase("negative control B: what librdkafka says about the unrelated CA");
    const std::string wrongCaLog = g_cfg.logPath + ".wrongca";
    std::remove(wrongCaLog.c_str());
    {
        ComponentObject consumer(lib, u"KafkaConsumer");
        applyConf(consumer, sslConfEntries(g_cfg.otherCaPath, g_cfg.client("wrongca-log")));
        // Initialize / SetLogFilePath / Initialize again - see case 4 for why
        // the path only takes effect on the second call.
        REQUIRE_MSG(consumer.callBool(u"Initialize", {g_cfg.bootstrap, g_cfg.group("wrongca")}),
                    "first Initialize failed: " + consumer.errorDescription());
        REQUIRE_MSG(consumer.callBool(u"SetLogFilePath", {wrongCaLog}),
                    "SetLogFilePath failed: " + consumer.errorDescription());
        REQUIRE(consumer.callBool(u"SetGlobalConf", {"debug", "security,broker"}));
        REQUIRE_MSG(consumer.callBool(u"Initialize", {g_cfg.bootstrap, g_cfg.group("wrongca")}),
                    "second Initialize failed: " + consumer.errorDescription());
        {
            // Any call that forces a connection will do; what is asserted is the
            // log, not this result.
            Value offsets = consumer.callFunc(u"QueryWatermarkOffsets",
                                              {g_cfg.topic, 0, g_cfg.negativeTimeoutMs});
            CHECK_MSG(offsets.isEmpty(),
                      "a client trusting only the unrelated CA read watermark offsets (" +
                          offsets.describe() + ")");
        }
        checkNoLeaks(consumer, "wrong-CA log consumer");
    }

    const LogWait rejected = waitForLogMarker(wrongCaLog, kMarkerHandshakeFailed, kLogWaitMs);
    for (const std::string& line : linesContaining(rejected.text, kMarkerHandshakeFailed, 2)) {
        test::note("librdkafka: " + clip(line, 300));
    }
    CHECK_MSG(rejected.found,
              std::string("librdkafka never logged \"") + kMarkerHandshakeFailed + "\" in " +
                  wrongCaLog + " within " + std::to_string(rejected.waitedMs) +
                  " ms (" + std::to_string(rejected.text.size()) +
                  " bytes of log), so the wrong-CA client failed for some reason other than a "
                  "rejected handshake");
    CHECK_MSG(mentionsCertificateRejection(rejected.text),
              "the wrong-CA client failed, but nothing in " + wrongCaLog +
                  " says the certificate was rejected. A control has to name its reason: without "
                  "one, any failure at all would satisfy it");

    //-----------------------------------------------------------------------//
    // (C) ...and the timeout those two ran under is not a lie.
    //-----------------------------------------------------------------------//
    // The positive counterpart: the same probe, the same address, the same
    // timeout, with the CORRECT settings - and it has to SUCCEED. A negative
    // control only says something when the positive version of the same call,
    // under the same conditions, works. Without this, a small
    // KAFKA_TEST_NEGATIVE_TIMEOUT_MS would make everything fail instantly, A
    // and B would both pass, and the suite would stay green having proved
    // nothing at all.
    //
    // It runs last on purpose. Failing it aborts nothing, so a run against, say,
    // a plaintext bootstrap port reports all three problems - "that port is not
    // TLS-only", "the failure was not a rejected certificate" and "the correct
    // configuration does not work either" - instead of only the first one it
    // happened to reach.
    phase("negative control C: the same probe with the CORRECT TLS configuration");
    const ProbeResult good =
        probeClusterMetadata(lib, sslConfEntries(g_cfg.caPath, g_cfg.client("control-ok")),
                             g_cfg.negativeTimeoutMs, "correct-TLS admin client");
    test::note(std::string("correct TLS: ") + (good.ok ? "cluster metadata in " : "FAILED after ") +
               std::to_string(good.spentMs) + " ms");
    CHECK_MSG(good.ok,
              "the same probe with the CORRECT SSL configuration failed in " +
                  std::to_string(good.spentMs) + " ms (" + good.detail + "): " + good.error +
                  ". Controls A and B above therefore prove nothing - they would have failed for "
                  "the same reason, whatever it is. Either KAFKA_TEST_NEGATIVE_TIMEOUT_MS "
                  "(currently " +
                  std::to_string(g_cfg.negativeTimeoutMs) +
                  " ms) is too short, or " + g_cfg.bootstrap +
                  " is not an SSL listener, or " + g_cfg.caPath + " is not the broker's CA");
    CHECK_EQ(good.error, std::string(kNoError));
}

//---------------------------------------------------------------------------//
// Case 2 - produce over TLS
//---------------------------------------------------------------------------//

void caseProduce(ComponentLibrary& lib)
{
    g_expected = buildMessages(g_cfg);
    const std::string payload = buildProduceJson(g_expected);

    phase("producer: creating KafkaProducer");
    ComponentObject producer(lib, u"KafkaProducer");

    phase("producer: SSL configuration (ca=" + g_cfg.caPath + ")");
    applySslConf(producer, g_cfg.client("producer"));

    // message.timeout.ms has to go through SetTopicConf. Producer1C::Initialize
    // calls GlobalConfDefaultInit, which does conf->set("default_topic_conf",
    // tconf) - that replaces the implicit topic conf librdkafka would have
    // created for a global message.timeout.ms, silently dropping the value.
    // It also matters a lot: Produce() blocks in
    //   while (producer->outq_len() > 0) producer->poll(1000);
    // with no timeout of its own (src/producer1c_core.cpp), so message.timeout.ms
    // is the only thing bounding it. At the librdkafka default that is 5 minutes.
    REQUIRE(producer.callBool(u"SetTopicConf",
                              {"message.timeout.ms", std::to_string(g_cfg.produceTimeoutMs)}));

    phase("producer: Initialize(" + g_cfg.bootstrap + ", " + g_cfg.topic + ", -1)");
    REQUIRE_MSG(producer.callBool(u"Initialize", {g_cfg.bootstrap, g_cfg.topic, -1}),
                "producer Initialize failed: " + producer.errorDescription());

    phase("producer: loading " + std::to_string(g_cfg.messageCount) + " messages");
    REQUIRE_MSG(producer.callBool(u"SetJSONMessageList", {payload}),
                "SetJSONMessageList rejected the payload: " + producer.errorDescription() + "; json=" +
                    clip(payload));
    CHECK_EQ(producer.callLong(u"GetMessagePoolLength"), static_cast<long>(g_cfg.messageCount));

    phase("producer: Produce() - blocks until every delivery report is back");
    const Clock::time_point begun = Clock::now();
    const bool produceOk = producer.callBool(u"Produce");
    const long long spentMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - begun).count();
    test::note("Produce() returned in " + std::to_string(spentMs) + " ms");
    REQUIRE_MSG(produceOk, "Produce() failed: " + producer.errorDescription());

    // Produce() clears the pool on success.
    CHECK_EQ(producer.callLong(u"GetMessagePoolLength"), 0L);

    const std::string report = producer.callString(u"GetJSONDeliveryReport");
    REQUIRE_MSG(!report.empty(),
                "GetJSONDeliveryReport returned nothing: " + producer.errorDescription());

    // Produce() returns true even when every single message timed out - the
    // delivery report is where the truth is, and IsDelivered() is its summary.
    const bool delivered = producer.callBool(u"IsDelivered");
    REQUIRE_MSG(delivered,
                "messages were not delivered over TLS. Delivery report:\n" + clip(report, 2000));

    phase("producer: verifying the delivery report");
    mini::Node parsed;
    try {
        parsed = mini::parse(report);
    } catch (const std::exception& e) {
        FAIL(std::string("delivery report is not valid JSON: ") + e.what() + "\n" + clip(report));
        return;
    }
    REQUIRE_MSG(parsed.isArray(), "delivery report is not a JSON array: " + clip(report));
    REQUIRE_MSG(parsed.items.size() == static_cast<std::size_t>(g_cfg.messageCount),
                "delivery report has " + std::to_string(parsed.items.size()) + " record(s), expected " +
                    std::to_string(g_cfg.messageCount) + "\n" + clip(report));

    // The report is in callback order, which need not match produce order, so
    // match on the key.
    for (const TestMessage& expected : g_expected) {
        const mini::Node* record = nullptr;
        for (const mini::Node& candidate : parsed.items) {
            if (candidate.str("Key") == expected.key) {
                record = &candidate;
                break;
            }
        }
        if (record == nullptr) {
            FAIL("no delivery report record for key '" + expected.key + "'\n" + clip(report));
            continue;
        }
        CHECK_EQ(record->str("Status"), std::string("Persisted"));
        CHECK_EQ(record->str("Error"), std::string("Success"));
        CHECK_EQ(record->str("Topic"), g_cfg.topic);
        CHECK_MSG(record->num("Partition") >= 0,
                  "key '" + expected.key + "' has partition " +
                      std::to_string(static_cast<long long>(record->num("Partition"))));
        CHECK_MSG(record->num("Offset") >= 0,
                  "key '" + expected.key + "' has offset " +
                      std::to_string(static_cast<long long>(record->num("Offset"))));
    }

    checkNoLeaks(producer, "producer");
    g_produced = true;
    test::note("produced " + std::to_string(g_cfg.messageCount) + " message(s) to '" + g_cfg.topic +
               "' over TLS, run tag " + g_cfg.runTag);
}

//---------------------------------------------------------------------------//
// Case 3 - consume the same messages back over TLS
//---------------------------------------------------------------------------//

struct ReceivedMessage
{
    std::string         key;
    std::string         value;
    std::string         topic;
    long long           offset = -1;
    long long           partition = -1;
    std::vector<Header> headers;
};

// ReceiveJSONMessages(false) returns
//   [{"Topic":..,"Partition":..,"Timestamp":..,"Offset":..,"Key":..,"Value":..,
//     "Headers":[{"Key":..,"Value":..}]}]
// Only this run's messages are kept: the topic survives between runs.
std::vector<ReceivedMessage> parsePool(const std::string& json, const std::string& keyPrefix)
{
    std::vector<ReceivedMessage> mine;
    const mini::Node root = mini::parse(json);
    if (!root.isArray()) {
        throw std::runtime_error("message pool is not a JSON array");
    }
    for (const mini::Node& record : root.items) {
        const std::string key = record.str("Key");
        if (key.compare(0, keyPrefix.size(), keyPrefix) != 0) {
            continue;
        }
        ReceivedMessage message;
        message.key = key;
        message.value = record.str("Value");
        message.topic = record.str("Topic");
        message.offset = static_cast<long long>(record.num("Offset"));
        message.partition = static_cast<long long>(record.num("Partition"));
        for (const mini::Node& header : record.array("Headers")) {
            message.headers.push_back(Header{header.str("Key"), header.str("Value")});
        }
        mine.push_back(std::move(message));
    }
    return mine;
}

void caseConsume(ComponentLibrary& lib)
{
    REQUIRE_MSG(g_produced, "the producer phase did not complete, there is nothing to consume");

    phase("consumer: creating KafkaConsumer");
    ComponentObject consumer(lib, u"KafkaConsumer");

    phase("consumer: SSL configuration");
    applySslConf(consumer, g_cfg.client("consumer"));
    REQUIRE(consumer.callBool(u"SetGlobalConf", {"enable.auto.commit", "false"}));
    // auto.offset.reset is a topic-level property; setting it globally would be
    // thrown away when Initialize installs the component's own topic conf.
    REQUIRE(consumer.callBool(u"SetTopicConf", {"auto.offset.reset", "earliest"}));

    phase("consumer: Initialize(" + g_cfg.bootstrap + ", " + g_cfg.group("consumer") + ")");
    REQUIRE_MSG(consumer.callBool(u"Initialize", {g_cfg.bootstrap, g_cfg.group("consumer")}),
                "consumer Initialize failed: " + consumer.errorDescription());

    // The poll loop below rebuilds the JSON from the message pool after every
    // batch and expects the pool to be cumulative. That only holds while this
    // property is off, which is the default - assert it rather than assume it.
    CHECK_FALSE(consumer.getProp(u"RemoveMessagesFromLocalQueueOnJSONBuild").asBool());

    // The positive counterpart of the plaintext control: the same call, the same
    // address, the same timeout - but with security.protocol=SSL it has to work.
    phase("consumer: QueryWatermarkOffsets over TLS");
    {
        Value offsets =
            consumer.callFunc(u"QueryWatermarkOffsets", {g_cfg.topic, 0, g_cfg.adminTimeoutMs});
        REQUIRE_MSG(offsets.isString(),
                    "QueryWatermarkOffsets over TLS failed (" + offsets.describe() +
                        "): " + consumer.errorDescription());
        const std::string text = offsets.asString();
        test::note("watermark offsets: " + text);
        try {
            const mini::Node node = mini::parse(text);
            // src/data_builder.cpp spells the high watermark "Hight".
            CHECK_MSG(node.num("Hight") >= g_cfg.messageCount,
                      "high watermark is " + std::to_string(static_cast<long long>(node.num("Hight"))) +
                          ", expected at least " + std::to_string(g_cfg.messageCount) +
                          " after producing");
            CHECK_MSG(node.num("Low") >= 0, "low watermark is negative: " + text);
        } catch (const std::exception& e) {
            FAIL(std::string("watermark offsets are not valid JSON: ") + e.what() + " -> " + text);
        }
    }

    phase("consumer: subscribing to '" + g_cfg.topic + "'");
    REQUIRE(consumer.callBool(u"AddTopicToSubscribeList", {g_cfg.topic}));
    REQUIRE_MSG(consumer.callBool(u"Subscribe"), "Subscribe failed: " + consumer.errorDescription());

    phase("consumer: polling for " + std::to_string(g_cfg.messageCount) + " message(s), deadline " +
          std::to_string(g_cfg.consumeTimeoutMs) + " ms");

    const Clock::time_point deadline =
        Clock::now() + std::chrono::milliseconds(g_cfg.consumeTimeoutMs);
    std::vector<ReceivedMessage> received;
    std::string                  pool;
    long                         lastPoolLength = -1;
    int                          polls = 0;
    std::string                  lastPollError;

    while (Clock::now() < deadline &&
           received.size() < static_cast<std::size_t>(g_cfg.messageCount)) {
        ++polls;
        // errors_count_to_interrupt must be >= 1: KafkaConsumerCore1C::ConsumePool
        // rejects 0 outright ("Bad parametrs: invalid interrupt errors count").
        // With 1, the batch stops at the first empty poll, so the outer loop
        // costs at most pollMs per iteration.
        const long rc = consumer.callLong(u"ConsumePool", {g_cfg.pollMs, g_cfg.messageCount, 1});
        if (rc == kConsumerFatalError) {
            FAIL("ConsumePool reported a fatal error (FatalError=" +
                 std::string(consumer.fatalError() ? "true" : "false") +
                 "): " + consumer.errorDescription());
            break;
        }
        if (rc == kConsumerError) {
            // "Local: Timed out" is the normal shape of an empty poll while the
            // group is still joining; it is not a failure on its own.
            lastPollError = consumer.errorDescription();
        } else if (rc != kConsumerNoError) {
            FAIL("ConsumePool returned an unexpected value: " + std::to_string(rc));
            break;
        }

        const long poolLength = consumer.callLong(u"GetMessagePoolLength");
        if (poolLength == lastPoolLength) {
            continue;   // nothing new, no point rebuilding the JSON
        }
        lastPoolLength = poolLength;

        pool = consumer.callString(u"ReceiveJSONMessages", {false});
        REQUIRE_MSG(!pool.empty(),
                    "ReceiveJSONMessages returned nothing: " + consumer.errorDescription());
        try {
            received = parsePool(pool, g_cfg.runTag);
        } catch (const std::exception& e) {
            FAIL(std::string("the message pool is not valid JSON: ") + e.what() + "\n" + clip(pool));
            return;
        }
        phase("consumer: " + std::to_string(received.size()) + "/" +
              std::to_string(g_cfg.messageCount) + " of this run matched (pool holds " +
              std::to_string(poolLength) + ")");
    }

    test::note(std::to_string(polls) + " ConsumePool call(s); last poll status: " +
               (lastPollError.empty() ? std::string("no error") : lastPollError));

    REQUIRE_MSG(received.size() == static_cast<std::size_t>(g_cfg.messageCount),
                "only " + std::to_string(received.size()) + " of " +
                    std::to_string(g_cfg.messageCount) + " message(s) came back within " +
                    std::to_string(g_cfg.consumeTimeoutMs) + " ms (run tag " + g_cfg.runTag +
                    "); last pool: " + clip(pool));

    phase("consumer: comparing keys, values and headers byte for byte");
    for (const TestMessage& expected : g_expected) {
        const ReceivedMessage* actual = nullptr;
        for (const ReceivedMessage& candidate : received) {
            if (candidate.key == expected.key) {
                actual = &candidate;
                break;
            }
        }
        if (actual == nullptr) {
            FAIL("key '" + expected.key + "' never arrived");
            continue;
        }
        CHECK_EQ(actual->value, expected.value);
        CHECK_EQ(actual->topic, g_cfg.topic);
        CHECK_MSG(actual->offset >= 0, "key '" + expected.key + "' has a negative offset");

        CHECK_MSG(actual->headers.size() == expected.headers.size(),
                  "key '" + expected.key + "' came back with " +
                      std::to_string(actual->headers.size()) + " header(s) " +
                      describeHeaders(actual->headers) + ", expected " +
                      std::to_string(expected.headers.size()) + " " +
                      describeHeaders(expected.headers));
        const std::size_t headerCount = std::min(actual->headers.size(), expected.headers.size());
        for (std::size_t h = 0; h < headerCount; ++h) {
            CHECK_EQ(actual->headers[h].key, expected.headers[h].key);
            CHECK_EQ(actual->headers[h].value, expected.headers[h].value);
        }
    }

    phase("consumer: Commit / Unsubscribe");
    CHECK_MSG(consumer.callBool(u"Commit"), "Commit failed: " + consumer.errorDescription());
    CHECK_MSG(consumer.callBool(u"Unsubscribe"), "Unsubscribe failed: " + consumer.errorDescription());
    CHECK_FALSE(consumer.fatalError());
    CHECK_MSG(consumer.callBool(u"ClearMessagePool"),
              "ClearMessagePool failed: " + consumer.errorDescription());
    CHECK_EQ(consumer.callLong(u"GetMessagePoolLength"), 0L);

    checkNoLeaks(consumer, "consumer");
}

//---------------------------------------------------------------------------//
// Case 4 - what librdkafka says the transport was
//---------------------------------------------------------------------------//

// The strongest single piece of evidence, straight from the library: with
// debug=security,broker librdkafka narrates its own handshakes, and the test
// asserts on the lines that only a FINISHED, VERIFIED handshake produces:
//
//   [thrd:ssl://localhost:9093/bootstrap]: ... Broker SSL certificate verified
//   [thrd:ssl://localhost:9093/bootstrap]: ... broker state change
//                                              SSL_HANDSHAKE -> APIVERSION_QUERY
//
// Two strings that look like they say the same thing, and do not:
//
//   * "(ssl) with socket" is logged at TCP-connect time, from the configured
//     protocol name, before a single handshake byte is exchanged. Measured on
//     this build: a run where EVERY handshake failed contained 103 of these
//     lines and no plaintext ones, and the old assertion passed. A successful
//     run has 3.
//   * "ssl://" matches every line in the log without exception - rd_kafka_log0
//     prefixes each one with [thrd:ssl://host:port/bootstrap]. As a fallback
//     disjunct it made the whole check unfailable.
//
// Both are still printed as context below; neither is asserted on. The two
// markers that are asserted on were 3 and 3 in a good run against 0 and 0 in
// the failed-handshake run, and the absence of "SSL handshake failed" is
// asserted as well, so a run where some connections succeed and others are
// rejected cannot pass either.
//
// Only KafkaConsumer can be made to produce any of this: it is the only class
// that registers an event_cb, and the log file is written by that callback.
void caseTransportEvidence(ComponentLibrary& lib)
{
    std::remove(g_cfg.logPath.c_str());   // MyEventCb opens the file in append mode

    phase("evidence: consumer with debug=security,broker -> " + g_cfg.logPath);
    ComponentObject consumer(lib, u"KafkaConsumer");
    applySslConf(consumer, g_cfg.client("evidence"));

    // SetLogFilePath is rejected with "Not initialized" before Initialize
    // (KafkaConsumerCore1C::SetLogFilePath checks IsInit()), but the path is
    // only consumed by Initialize itself. So: Initialize, set the path, then
    // Initialize again - the second call recreates the client with logging on.
    REQUIRE_MSG(consumer.callBool(u"Initialize", {g_cfg.bootstrap, g_cfg.group("evidence")}),
                "first Initialize failed: " + consumer.errorDescription());
    REQUIRE_MSG(consumer.callBool(u"SetLogFilePath", {g_cfg.logPath}),
                "SetLogFilePath failed: " + consumer.errorDescription());
    REQUIRE(consumer.callBool(u"SetGlobalConf", {"debug", "security,broker"}));
    REQUIRE_MSG(consumer.callBool(u"Initialize", {g_cfg.bootstrap, g_cfg.group("evidence")}),
                "second Initialize failed: " + consumer.errorDescription());

    phase("evidence: forcing a broker round trip");
    // Scoped, like the identical call in caseConsume: the Value owns the string
    // the component allocated through the memory manager and only hands it back
    // on destruction. Leaving it alive until the end of the function would make
    // checkNoLeaks below report that one perfectly legitimate block as a leak.
    {
        Value offsets =
            consumer.callFunc(u"QueryWatermarkOffsets", {g_cfg.topic, 0, g_cfg.adminTimeoutMs});
        CHECK_MSG(offsets.isString(),
                  "QueryWatermarkOffsets over TLS failed while capturing the log (" +
                      offsets.describe() + "): " + consumer.errorDescription());
    }

    // The callback writes from librdkafka's threads, one open/append/close per
    // event, so the file trails the call that caused it. A fixed sleep was the
    // wrong tool: on a loaded box the tail can still be missing, and a truncated
    // log reads exactly like proof of no plaintext. Poll for the marker instead,
    // and fail on the deadline rather than draw a conclusion from half a file.
    phase("evidence: waiting for " + g_cfg.logPath + " to show the finished handshake");
    const LogWait wait = waitForLogMarker(g_cfg.logPath, kMarkerCertVerified, kLogWaitMs);
    const std::string log = wait.text;

    phase("evidence: reading " + g_cfg.logPath);
    REQUIRE_MSG(!log.empty(),
                "the librdkafka log at " + g_cfg.logPath +
                    " is empty - SetLogFilePath did not take effect, so the transport could not be "
                    "confirmed from the library's own output");
    REQUIRE_MSG(wait.found,
                std::string("librdkafka never logged \"") + kMarkerCertVerified + "\" in " +
                    g_cfg.logPath + " within " + std::to_string(wait.waitedMs) + " ms (" +
                    std::to_string(log.size()) +
                    " bytes of log). Either no TLS handshake completed, or the log is still being "
                    "written - and a partial log must not be read as evidence either way");

    const std::size_t certVerified    = countOccurrences(log, kMarkerCertVerified);
    const std::size_t handshakesDone  = countOccurrences(log, kMarkerHandshakeDone);
    const std::size_t handshakesFailed = countOccurrences(log, kMarkerHandshakeFailed);
    const std::size_t plainConnects   = countOccurrences(log, "(plaintext) with socket");
    // Context only - see the comment above this function for why neither of
    // these is an assertion.
    const std::size_t sslConnects     = countOccurrences(log, "(ssl) with socket");

    for (const std::string& line : linesContaining(log, kMarkerCertVerified, 2)) {
        test::note("librdkafka: " + clip(line, 300));
    }
    for (const std::string& line : linesContaining(log, kMarkerHandshakeFailed, 2)) {
        test::note("librdkafka: " + clip(line, 300));
    }

    CHECK_MSG(certVerified > 0,
              std::string("librdkafka never logged \"") + kMarkerCertVerified + "\" in " +
                  g_cfg.logPath +
                  ", so no broker certificate was verified and nothing here shows a TLS handshake "
                  "completed");
    CHECK_MSG(handshakesDone > 0,
              std::string("librdkafka never logged \"") + kMarkerHandshakeDone + "\" in " +
                  g_cfg.logPath +
                  ", so no connection ever left the handshake state for the protocol proper");
    CHECK_MSG(handshakesFailed == 0,
              std::to_string(handshakesFailed) + " TLS handshake(s) failed (\"" +
                  kMarkerHandshakeFailed + "\") while security.protocol=SSL; see " + g_cfg.logPath);
    CHECK_MSG(plainConnects == 0,
              std::to_string(plainConnects) +
                  " connection(s) were made in plaintext while security.protocol=SSL; see " +
                  g_cfg.logPath);

    test::note("librdkafka log kept at " + g_cfg.logPath + " (" + std::to_string(log.size()) +
               " bytes, " + std::to_string(certVerified) + " verified certificate(s), " +
               std::to_string(handshakesDone) + " completed handshake(s), " +
               std::to_string(handshakesFailed) + " failed, " + std::to_string(sslConnects) +
               " ssl connect attempt(s), " + std::to_string(plainConnects) + " plaintext)");

    checkNoLeaks(consumer, "evidence consumer");
}

//---------------------------------------------------------------------------//
// Case 5 - the broker version this run is entitled to claim
//---------------------------------------------------------------------------//

// "TLS against a Kafka 4.x broker" is the headline of this whole file, and for
// a long time it was only prose: the documented --external-broker path printed
// it verbatim while talking to whatever was on the other end, 3.x included.
// tests/docker/up.sh exports KAFKA_TEST_BROKER_VERSION; this turns the headline
// into an assertion.
void caseBrokerVersion()
{
    if (g_cfg.brokerVersion.empty()) {
        // Not a failure: pointing the test at an existing broker is supported,
        // and the person doing it may not know its version. The banner says
        // UNVERIFIED and this note is the only honest thing that can be said,
        // so such a run cannot be quoted as evidence about a Kafka generation.
        test::note("KAFKA_TEST_BROKER_VERSION is not set: this run says NOTHING about the broker "
                   "version. tests/docker/up.sh exports it; set it by hand for an external "
                   "broker before quoting the result as Kafka " +
                   g_cfg.expectBrokerMajor + ".x evidence.");
        return;
    }

    const std::string major = majorVersion(g_cfg.brokerVersion);
    REQUIRE_MSG(!major.empty(), "KAFKA_TEST_BROKER_VERSION='" + g_cfg.brokerVersion +
                                    "' does not begin with a version number");
    test::note("broker version " + g_cfg.brokerVersion + " (major " + major + ")");
    CHECK_MSG(major == g_cfg.expectBrokerMajor,
              "the broker under test is version " + g_cfg.brokerVersion + " (major " + major +
                  "), but this run claims to test a Kafka " + g_cfg.expectBrokerMajor +
                  ".x broker. Either point it at the right broker, or set "
                  "KAFKA_TEST_EXPECT_BROKER_MAJOR=" + major + " if testing " + major +
                  ".x is what you meant");
}

//---------------------------------------------------------------------------//
// Round-trip helper shared by the two byte-exactness cases below
//---------------------------------------------------------------------------//

// Reads exactly one already-produced message back out of the topic with a
// throwaway consumer group, and hands the caller BOTH the raw pool text and
// the parsed record.
//
// The raw text matters: several of the defects this suite guards against are
// one wrong character in what ReceiveJSONMessages emits, and a case that only
// ever looks at the parsed result cannot see them.
//
// `groupTag` picks a private consumer group so this never disturbs the offsets
// of the main consume case; `keyPrefix` is what parsePool() filters on and must
// NOT be g_cfg.runTag, or caseConsume would count these messages as its own.
// Returns false (having already reported the failure) if the message never
// arrived or the pool did not parse; `rawPool` is filled either way whenever
// anything at all came back.
bool consumeOneByKey(ComponentLibrary&  lib,
                     const std::string& groupTag,
                     const std::string& keyPrefix,
                     const std::string& wantedKey,
                     ReceivedMessage&   out,
                     std::string&       rawPool)
{
    ComponentObject consumer(lib, u"KafkaConsumer");
    applySslConf(consumer, g_cfg.client(groupTag));
    REQUIRE(consumer.callBool(u"SetGlobalConf", {"enable.auto.commit", "false"}));
    REQUIRE(consumer.callBool(u"SetTopicConf", {"auto.offset.reset", "earliest"}));
    REQUIRE_MSG(consumer.callBool(u"Initialize", {g_cfg.bootstrap, g_cfg.group(groupTag)}),
                "consumer Initialize failed: " + consumer.errorDescription());
    REQUIRE(consumer.callBool(u"AddTopicToSubscribeList", {g_cfg.topic}));
    REQUIRE_MSG(consumer.callBool(u"Subscribe"),
                "Subscribe failed: " + consumer.errorDescription());

    const Clock::time_point deadline =
        Clock::now() + std::chrono::milliseconds(g_cfg.consumeTimeoutMs);

    bool        found      = false;
    bool        parsed     = true;
    std::string parseError;
    long        lastLength = -1;

    while (Clock::now() < deadline && !found) {
        const long rc = consumer.callLong(u"ConsumePool", {g_cfg.pollMs, 500, 1});
        if (rc == kConsumerFatalError) {
            FAIL("ConsumePool reported a fatal error while looking for '" + clip(wantedKey, 120) +
                 "': " + consumer.errorDescription());
            return false;
        }
        const long poolLength = consumer.callLong(u"GetMessagePoolLength");
        if (poolLength == 0 || poolLength == lastLength) {
            continue;
        }
        lastLength = poolLength;

        rawPool = consumer.callString(u"ReceiveJSONMessages", {false});
        if (rawPool.empty()) {
            continue;
        }
        try {
            parsed     = true;
            parseError.clear();
            for (const ReceivedMessage& candidate : parsePool(rawPool, keyPrefix)) {
                if (candidate.key == wantedKey) {
                    out   = candidate;
                    found = true;
                    break;
                }
            }
        } catch (const std::exception& e) {
            // Stop here: a later, larger pool cannot parse either if this one
            // did not. The error is reported below, and rawPool still holds the
            // bytes so the caller can assert on them.
            parsed     = false;
            parseError = e.what();
            break;
        }
    }

    if (!parsed) {
        FAIL("the message pool is not valid JSON: " + parseError + "\n" + clip(rawPool));
        return false;
    }
    if (!found) {
        FAIL("'" + clip(wantedKey, 120) + "' did not come back within " +
             std::to_string(g_cfg.consumeTimeoutMs) + " ms; last pool: " + clip(rawPool));
        return false;
    }
    checkNoLeaks(consumer, "round-trip consumer");
    return true;
}

//---------------------------------------------------------------------------//
// Case 6 - UTF-8 conversion across the 1C boundary (was a pinned component bug)
//---------------------------------------------------------------------------//

// Everything a method receives crosses the 1C boundary as UTF-16 and is
// converted by ComponentBase::toUTF8String. That function used to size its
// output buffer at
//
//     size_t dest_bytes = (len + 1) * sizeof(WCHAR_T);   // 2 bytes per unit
//     char *dst = new char[dest_bytes];
//     storeUTF16LEtoUTF8(src, len, dst, dest_bytes);     // return value ignored
//     res = std::string(dst);
//
// i.e. two UTF-8 bytes per UTF-16 code unit. In UTF-8 that is one byte short
// for every character in U+0800..U+FFFF (CJK, the em dash, the ellipsis...),
// so iconv stopped with E2BIG, storeUTF16LEtoUTF8 returned false, the caller
// ignored the return value, and the half-converted buffer was used as if it
// were the string. Two distinct failures came out of that one bound:
//
//   * silent truncation - a string longer than the budget came back cut, and
//     a JSON document cut mid-string is simply rejected by the reader;
//   * a heap-buffer-overflow - storeUTF16LEtoUTF8 memsets the buffer first, so
//     a truncated result is USUALLY still terminated by leftover zeros, but
//     when the output fills the budget EXACTLY there are no leftover zeros and
//     std::string(dst) ran strlen off the end of the allocation.
//
// The buffer is now sized at len * 3 + 1 - the true upper bound, since a BMP
// unit yields at most three UTF-8 bytes and a surrogate pair (two units)
// yields four - the conversion result is no longer ignored, and the std::string
// is built from the byte count the converter reports rather than by scanning
// for a NUL that may not be there.
//
// This case is no longer a pin. It is the regression guard: both halves assert
// that 3-byte characters survive, and (b) deliberately keeps the two payloads
// that used to be the truncation and the exact-fill crash, because those are
// the inputs worth never losing coverage of.
void caseThreeByteUtf8(ComponentLibrary& lib)
{
    // (b) first, because it needs no broker at all: SetJSONMessageList converts
    // the whole JSON document in one call, so a value made of 3-byte characters
    // used to take the document itself over budget.
    phase("utf-8: 3-byte-heavy payloads through SetJSONMessageList");
    {
        // 20 repeats of three CJK characters: 60 characters, 180 UTF-8 bytes,
        // 60 UTF-16 code units.
        std::string heavy;
        for (int i = 0; i < 20; ++i) {
            heavy += "\xE6\x97\xA5\xE6\x9C\xAC\xE8\xAA\x9E";   // 3 CJK chars, 3 bytes each
        }

        struct HeavyCase
        {
            const char* key;
            const char* whatItUsedToDo;
        };
        // The two keys differ by two ASCII characters, and that used to decide
        // WHICH of the two bugs fired:
        //
        //   "u8"   - 85 UTF-16 units, a 172-byte budget, and iconv wrote
        //            exactly 172 bytes (25 ASCII + 49 whole CJK characters).
        //            No leftover zero, so std::string(dst) ran strlen past the
        //            end: an ASan build reported
        //                heap-buffer-overflow ... READ of size 173 in strlen
        //                ComponentBase::toUTF8String
        //                  -> Producer1C::SetJSONMessageList
        //            This spelling could NOT be used in this file before the
        //            fix - it would have aborted the whole suite under
        //            -fsanitize=address - which is exactly why it is here now.
        //   "u8xx" - two extra ASCII characters move the cut short of the end,
        //            so the old code truncated quietly instead of overrunning,
        //            the document ended mid-string and SetJSONMessageList came
        //            back false with "JSON parsing: invalid json".
        //
        // Both must now be accepted, and both must land exactly one message in
        // the pool.
        static const HeavyCase kHeavyCases[] = {
            {"u8",   "overran the conversion buffer (exact fill, no NUL left)"},
            {"u8xx", "was truncated mid-string and rejected as invalid JSON"},
        };

        for (const HeavyCase& heavyCase : kHeavyCases) {
            ComponentObject producer(lib, u"KafkaProducer");

            const std::string payload = "[{\"Key\":\"" + std::string(heavyCase.key) +
                                        "\",\"Value\":" + mini::quoted(heavy) + "}]";

            const bool        loaded = producer.callBool(u"SetJSONMessageList", {payload});
            const std::string error  = producer.errorDescription();
            test::note(std::string("key \"") + heavyCase.key + "\", 3-byte-heavy payload (" +
                       std::to_string(payload.size()) + " UTF-8 bytes): SetJSONMessageList -> " +
                       (loaded ? std::string("true") : std::string("false")) + ", " + error);

            CHECK_MSG(loaded,
                      std::string("SetJSONMessageList rejected a 3-byte-heavy payload with key \"") +
                          heavyCase.key + "\" - it " + heavyCase.whatItUsedToDo +
                          " before the toUTF8String fix, and must now be accepted: " + error);
            CHECK_EQ(error, std::string(kNoError));
            CHECK_EQ(producer.callLong(u"GetMessagePoolLength"), 1L);

            checkNoLeaks(producer, "3-byte-heavy producer");
        }
    }

    // (a) The bytes have to survive the whole way, not merely be accepted.
    // The document goes 1C UTF-16 -> toUTF8String -> librdkafka -> the broker,
    // and the key comes back out of the delivery report as the broker echoed
    // it; the value is then read back with a real consumer and compared byte
    // for byte. An em dash (3 bytes) sits next to a run of CJK so the case
    // covers both "one 3-byte character among ASCII" - which always worked -
    // and "mostly 3-byte characters" - which did not.
    phase("utf-8: 3-byte characters produced over TLS and read back byte for byte");
    {
        TestMessage message;
        message.key = "utf8-" + g_cfg.runTag + "-emdash-\xE2\x80\x94";
        message.value =
            "em dash \xE2\x80\x94 among ASCII, then 3-byte-heavy: "
            "\xE6\x97\xA5\xE6\x9C\xAC\xE8\xAA\x9E\xE6\x97\xA5\xE6\x9C\xAC\xE8\xAA\x9E"
            "\xE6\x97\xA5\xE6\x9C\xAC\xE8\xAA\x9E\xE6\x97\xA5\xE6\x9C\xAC\xE8\xAA\x9E"
            "\xE6\x97\xA5\xE6\x9C\xAC\xE8\xAA\x9E \xE2\x80\xA6 and a 4-byte one: "
            "\xF0\x9F\x94\x91";   // U+1F511, a surrogate pair on the 1C side
        message.headers.push_back(Header{"onec-test-utf8", "\xE6\x97\xA5\xE6\x9C\xAC\xE8\xAA\x9E"});

        const std::string payload = buildProduceJson({message});

        ComponentObject producer(lib, u"KafkaProducer");
        applySslConf(producer, g_cfg.client("utf8"));
        REQUIRE(producer.callBool(u"SetTopicConf",
                                  {"message.timeout.ms", std::to_string(g_cfg.produceTimeoutMs)}));
        REQUIRE_MSG(producer.callBool(u"Initialize", {g_cfg.bootstrap, g_cfg.topic, -1}),
                    "producer Initialize failed: " + producer.errorDescription());
        REQUIRE_MSG(producer.callBool(u"SetJSONMessageList", {payload}),
                    "SetJSONMessageList rejected a payload with 3-byte characters in it: " +
                        producer.errorDescription());
        REQUIRE_MSG(producer.callBool(u"Produce"),
                    "Produce() failed: " + producer.errorDescription());
        REQUIRE_MSG(producer.callBool(u"IsDelivered"),
                    "the 3-byte message was not delivered: " +
                        clip(producer.callString(u"GetJSONDeliveryReport"), 1000));

        const std::string report = producer.callString(u"GetJSONDeliveryReport");
        mini::Node        parsed;
        try {
            parsed = mini::parse(report);
        } catch (const std::exception& e) {
            FAIL(std::string("delivery report is not valid JSON: ") + e.what() + "\n" +
                 clip(report));
            return;
        }
        REQUIRE_MSG(parsed.isArray() && parsed.items.size() == 1,
                    "expected exactly one delivery record: " + clip(report));
        // The key in the report is the one the BROKER echoed back
        // (src/producer1c_delivery_report.cpp:33 reads message.key()), so this
        // compares the bytes after a full round trip, not the ones we sent.
        CHECK_EQ(parsed.items[0].str("Key"), message.key);
        CHECK_EQ(parsed.items[0].str("Status"), std::string("Persisted"));

        checkNoLeaks(producer, "3-byte producer");

        // ...and out through the consumer, which is the only way to prove the
        // VALUE survived: the delivery report carries the key alone.
        ReceivedMessage back;
        std::string     pool;
        if (!consumeOneByKey(lib, "utf8", "utf8-" + g_cfg.runTag, message.key, back, pool)) {
            return;
        }
        CHECK_EQ(back.value, message.value);
        REQUIRE_MSG(back.headers.size() == 1,
                    "expected one header, got " + std::to_string(back.headers.size()) + ": " +
                        clip(pool));
        CHECK_EQ(back.headers[0].value, message.headers[0].value);
    }
}

//---------------------------------------------------------------------------//
// Case 7 - C0 control characters in a message (was a malformed-JSON bug)
//---------------------------------------------------------------------------//

// DataBuilder::escape_string_simple used to emit the 'u' of a \uXXXX escape
// twice - buffer->append("\\u") followed by sprintf(buff, "u%04x", ...) - so a
// control byte came out as the 7-character \uu001f. \uu is not a legal JSON
// escape, so ONE control byte anywhere in the batch made the WHOLE string
// ReceiveJSONMessages hands to 1C unparseable, not just that field. It is
// reachable for the message key, the message value and header keys and values,
// and all four EscapeMessage* properties default to true.
//
// The triggering bytes are the C0 controls JSON has no short escape for:
// 0x00-0x07, 0x0B, 0x0E-0x1F. 0x1F (unit separator) and 0x01 are ordinary
// in-payload field delimiters, so this was not a theoretical byte.
//
// The suite had no control-character message at all before this case, which is
// why the defect survived a 176-check TLS run. It asserts the raw pool bytes,
// not only that a parser accepts them, because "\uu001f" and "" differ by
// exactly one character and a lenient reader could hide that.
void caseControlCharacters(ComponentLibrary& lib)
{
    phase("json: a C0 control byte in key, value and header survives as \\u001f");

    TestMessage message;
    // Deliberately NOT prefixed with runTag: parsePool() in caseConsume filters
    // by that prefix and this message must never be counted as one of the run's
    // expected messages.
    message.key   = "ctl-" + g_cfg.runTag + "-key\x1F" "end";
    message.value = "ctl-" + g_cfg.runTag + "-value\x1F" "end\x01" "-" +
                    std::string("\xD0\xBF\xD1\x80\xD0\xBE\xD0\xB2\xD0\xB5\xD1\x80\xD0\xBA\xD0\xB0");
    // 0x0B (vertical tab) has no short JSON escape either, and 0x07 (bell) sits
    // at the bottom of the range; both go through the same default: branch.
    message.headers.push_back(Header{"ctl-probe", "hdr\x1F" "end\x0B" "\x07"});

    const std::string payload = buildProduceJson({message});

    ComponentObject producer(lib, u"KafkaProducer");
    applySslConf(producer, g_cfg.client("ctl"));
    REQUIRE(producer.callBool(u"SetTopicConf",
                              {"message.timeout.ms", std::to_string(g_cfg.produceTimeoutMs)}));
    REQUIRE_MSG(producer.callBool(u"Initialize", {g_cfg.bootstrap, g_cfg.topic, -1}),
                "producer Initialize failed: " + producer.errorDescription());
    REQUIRE_MSG(producer.callBool(u"SetJSONMessageList", {payload}),
                "SetJSONMessageList rejected a payload with control characters: " +
                    producer.errorDescription());
    REQUIRE_MSG(producer.callBool(u"Produce"), "Produce() failed: " + producer.errorDescription());
    REQUIRE_MSG(producer.callBool(u"IsDelivered"),
                "the control-character message was not delivered: " +
                    clip(producer.callString(u"GetJSONDeliveryReport"), 1000));
    checkNoLeaks(producer, "control-character producer");

    ReceivedMessage back;
    std::string     pool;
    // Deliberately not an early return: if the pool does not parse, THAT is the
    // defect coming back, and the raw-byte assertions below are the ones that
    // name it. consumeOneByKey has already reported the parse failure and has
    // left the bytes it got in `pool`.
    const bool roundTripped =
        consumeOneByKey(lib, "ctl", "ctl-" + g_cfg.runTag, message.key, back, pool);

    // The bytes the component actually emitted. This is the assertion that
    // would have caught the defect on the day it was written.
    CHECK_MSG(pool.find("\\uu00") == std::string::npos,
              "the message pool still contains the malformed escape \\uu00xx: " + clip(pool));
    CHECK_MSG(pool.find("\\u001f") != std::string::npos,
              "expected the unit separator to be escaped as \\u001f in the raw pool: " +
                  clip(pool));
    CHECK_MSG(pool.find("\\u0001") != std::string::npos,
              "expected 0x01 to be escaped as \\u0001 in the raw pool: " + clip(pool));
    CHECK_MSG(pool.find("\\u000b") != std::string::npos,
              "expected 0x0b to be escaped as \\u000b in the raw pool: " + clip(pool));

    if (!roundTripped) {
        return;
    }

    // ...and that the escape means what it says: the bytes come back identical.
    CHECK_EQ(back.key, message.key);
    CHECK_EQ(back.value, message.value);
    REQUIRE_MSG(back.headers.size() == 1,
                "expected one header, got " + std::to_string(back.headers.size()) + ": " +
                    clip(pool));
    CHECK_EQ(back.headers[0].key, message.headers[0].key);
    CHECK_EQ(back.headers[0].value, message.headers[0].value);

    // The 1C-internal format never \u-escapes; it doubles the quote and passes
    // everything else through raw. That is a separate, deliberate design choice
    // and the fix must not have moved it - so the raw control byte is still
    // expected there.
    phase("json: the 1C-internal format still passes control bytes through raw");
    {
        ComponentObject consumer(lib, u"KafkaConsumer");
        applySslConf(consumer, g_cfg.client("ctl-internal"));
        REQUIRE(consumer.callBool(u"SetGlobalConf", {"enable.auto.commit", "false"}));
        REQUIRE(consumer.callBool(u"SetTopicConf", {"auto.offset.reset", "earliest"}));
        REQUIRE_MSG(consumer.callBool(u"Initialize",
                                      {g_cfg.bootstrap, g_cfg.group("ctl-internal")}),
                    "consumer Initialize failed: " + consumer.errorDescription());
        REQUIRE(consumer.callBool(u"AddTopicToSubscribeList", {g_cfg.topic}));
        REQUIRE_MSG(consumer.callBool(u"Subscribe"),
                    "Subscribe failed: " + consumer.errorDescription());

        const Clock::time_point deadline =
            Clock::now() + std::chrono::milliseconds(g_cfg.consumeTimeoutMs);
        std::string internal;
        while (Clock::now() < deadline) {
            consumer.callLong(u"ConsumePool", {g_cfg.pollMs, 200, 1});
            if (consumer.callLong(u"GetMessagePoolLength") == 0L) {
                continue;
            }
            internal = consumer.callString(u"ReceiveOnecInternalMessages", {false});
            if (internal.find(message.key) != std::string::npos) {
                break;
            }
        }
        REQUIRE_MSG(internal.find(message.key) != std::string::npos,
                    "the control-character message did not come back in the 1C-internal "
                    "format within " + std::to_string(g_cfg.consumeTimeoutMs) + " ms: " +
                        clip(internal));
        CHECK_MSG(internal.find("\\u001f") == std::string::npos,
                  "the 1C-internal format started \\u-escaping - it never did: " +
                      clip(internal));
        CHECK_MSG(internal.find('\x1F') != std::string::npos,
                  "the raw 0x1f byte is missing from the 1C-internal format: " + clip(internal));
        checkNoLeaks(consumer, "control-character internal consumer");
    }
}

//---------------------------------------------------------------------------//
// Cases 8-10 - payloads the component's own producer cannot express, and
//              payloads whose BYTES the JSON pool used to lose
//---------------------------------------------------------------------------//

// Where kafka_null_producer lives: next to this executable, because CMake puts
// both in the same build directory.
std::string executableDir()
{
    std::string   buffer(4096, '\0');
    const ssize_t written = ::readlink("/proc/self/exe", &buffer[0], buffer.size() - 1);
    if (written <= 0) {
        return std::string(".");
    }
    buffer.resize(static_cast<std::size_t>(written));
    const std::size_t slash = buffer.find_last_of('/');
    return slash == std::string::npos ? std::string(".") : buffer.substr(0, slash);
}

// fork + execv, with the child's stdout and stderr captured into a file. A pipe
// would need a reader loop to avoid filling up; a file does not, and the output
// is a handful of lines. Only async-signal-safe calls happen between fork() and
// execv(), which matters because librdkafka's threads are already running in
// this process by the time this is called.
bool runHelper(const std::string& program, const std::vector<std::string>& args,
               std::string* output)
{
    const std::string logPath = "/tmp/onec-null-producer-" + std::to_string(::getpid()) + ".log";

    std::vector<char*> argv;
    argv.push_back(const_cast<char*>(program.c_str()));
    for (const std::string& arg : args) {
        argv.push_back(const_cast<char*>(arg.c_str()));
    }
    argv.push_back(nullptr);

    std::cout.flush();
    const pid_t pid = ::fork();
    if (pid < 0) {
        *output = std::string("fork() failed: ") + std::strerror(errno);
        return false;
    }
    if (pid == 0) {
        const int fd = ::open(logPath.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
        if (fd >= 0) {
            ::dup2(fd, 1);
            ::dup2(fd, 2);
            ::close(fd);
        }
        ::execv(program.c_str(), argv.data());
        ::_exit(127);
    }

    int status = 0;
    while (::waitpid(pid, &status, 0) < 0) {
        if (errno != EINTR) {
            *output = std::string("waitpid() failed: ") + std::strerror(errno);
            return false;
        }
    }
    *output = readFile(logPath);
    ::unlink(logPath.c_str());

    if (!WIFEXITED(status)) {
        *output += "\n(the helper did not exit normally)";
        return false;
    }
    if (WEXITSTATUS(status) != 0) {
        *output += "\n(the helper exited with status " + std::to_string(WEXITSTATUS(status)) + ")";
        return false;
    }
    return true;
}

// A topic nobody else writes to, so a case can assert on the WHOLE pool instead
// of filtering. The broker auto-creates it with one partition
// (KAFKA_AUTO_CREATE_TOPICS_ENABLE / KAFKA_NUM_PARTITIONS in
// tests/docker/compose.yml), and it disappears with the volume at down.sh.
std::string scratchTopic(const std::string& suffix)
{
    return g_cfg.topic + "-" + suffix + "-" + g_cfg.runTag;
}

struct BoolProp
{
    const char16_t* name;
    bool            value;
};

// Everything one of these cases needs to see about a single ReceiveJSONMessages
// call: the bytes, the error text, and whether the batch survived the call.
struct PoolRead
{
    bool        reached = false;        // the poll loop saw `expected` messages
    long        lengthBefore = 0;
    long        lengthAfter = 0;
    TYPEVAR     variantType = VTYPE_EMPTY;
    std::string pool;
    std::string error;
};

PoolRead readTopicPool(ComponentLibrary& lib, const std::string& groupTag,
                       const std::string& topic, long expected,
                       const std::vector<BoolProp>& props, bool base64)
{
    PoolRead        out;
    ComponentObject consumer(lib, u"KafkaConsumer");
    applySslConf(consumer, g_cfg.client(groupTag));
    REQUIRE(consumer.callBool(u"SetGlobalConf", {"enable.auto.commit", "false"}));
    REQUIRE(consumer.callBool(u"SetTopicConf", {"auto.offset.reset", "earliest"}));
    for (const BoolProp& prop : props) {
        consumer.setProp(prop.name, prop.value);
    }
    REQUIRE_MSG(consumer.callBool(u"Initialize", {g_cfg.bootstrap, g_cfg.group(groupTag)}),
                "consumer Initialize failed: " + consumer.errorDescription());
    REQUIRE(consumer.callBool(u"AddTopicToSubscribeList", {topic}));
    REQUIRE_MSG(consumer.callBool(u"Subscribe"), "Subscribe failed: " + consumer.errorDescription());

    const Clock::time_point deadline =
        Clock::now() + std::chrono::milliseconds(g_cfg.consumeTimeoutMs);
    while (Clock::now() < deadline) {
        const long rc = consumer.callLong(u"ConsumePool", {g_cfg.pollMs, 500, 1});
        if (rc == kConsumerFatalError) {
            FAIL("ConsumePool reported a fatal error on '" + topic +
                 "': " + consumer.errorDescription());
            return out;
        }
        if (consumer.callLong(u"GetMessagePoolLength") >= expected) {
            break;
        }
    }

    out.lengthBefore = consumer.callLong(u"GetMessagePoolLength");
    if (out.lengthBefore < expected) {
        FAIL("only " + std::to_string(out.lengthBefore) + " of " + std::to_string(expected) +
             " message(s) arrived from '" + topic + "' within " +
             std::to_string(g_cfg.consumeTimeoutMs) + " ms");
        return out;
    }
    out.reached = true;

    {
        // Its own scope: Value releases the block it owns on destruction, and
        // the leak check below is only meaningful once it has.
        const Value value = consumer.callFunc(u"ReceiveJSONMessages", {base64});
        out.variantType = value.type();
        // asString() throws on VTYPE_EMPTY, which is one of the answers a failed
        // hand-over produces, so the type is recorded and the bytes are only
        // read when there are any.
        out.pool = value.isString() ? value.asString() : std::string();
    }
    out.error = consumer.errorDescription();
    out.lengthAfter = consumer.callLong(u"GetMessagePoolLength");

    checkNoLeaks(consumer, ("pool reader for " + topic).c_str());
    return out;
}

// One message produced through the component with DecodeBase64Value on, which
// is the only way a 1C script can put arbitrary BYTES - a 0x00, an ill-formed
// UTF-8 sequence - into a Kafka record.
bool produceBase64Value(ComponentLibrary& lib, const std::string& topicName,
                        const std::string& clientTag, const std::string& key,
                        const std::string& base64Value)
{
    ComponentObject producer(lib, u"KafkaProducer");
    applySslConf(producer, g_cfg.client(clientTag));
    REQUIRE_MSG(producer.callBool(u"SetTopicConf",
                                  {"message.timeout.ms", std::to_string(g_cfg.produceTimeoutMs)}),
                "SetTopicConf failed: " + producer.errorDescription());
    producer.setProp(u"DecodeBase64Value", true);
    REQUIRE_MSG(producer.callBool(u"Initialize", {g_cfg.bootstrap, topicName, -1}),
                "producer Initialize failed: " + producer.errorDescription());

    const std::string payload =
        "[{\"Key\":" + mini::quoted(key) + ",\"Value\":" + mini::quoted(base64Value) + "}]";
    REQUIRE_MSG(producer.callBool(u"SetJSONMessageList", {payload}),
                "SetJSONMessageList failed: " + producer.errorDescription());
    REQUIRE_MSG(producer.callBool(u"Produce"), "Produce failed: " + producer.errorDescription());
    REQUIRE_MSG(producer.callBool(u"IsDelivered"),
                "the message was not delivered: " + clip(producer.callString(u"GetJSONDeliveryReport"), 600));
    checkNoLeaks(producer, "base64 producer");
    return true;
}

// Finds the record with this Key in a parsed pool. Returns nullptr when absent.
const mini::Node* recordWithKey(const mini::Node& pool, const std::string& key)
{
    for (const mini::Node& record : pool.items) {
        if (record.str("Key") == key) {
            return &record;
        }
    }
    return nullptr;
}

const mini::Node* headerWithKey(const mini::Node& record, const std::string& key)
{
    for (const mini::Node& header : record.array("Headers")) {
        if (header.str("Key") == key) {
            return &header;
        }
    }
    return nullptr;
}

//---------------------------------------------------------------------------//
// Case 8 - a tombstone and a null header value (was invalid JSON)
//---------------------------------------------------------------------------//

// DataBuilder wrote an unescaped message value straight after the colon:
//
//     "Value":<the bytes>
//
// For a Kafka TOMBSTONE - a record whose value is ABSENT, which is how Kafka
// spells a deletion in a compacted topic - there are no bytes, so the document
// came out as
//
//     ..."Key":"k","Value":,"Headers":[...]
//
// and `"Value":,` is not JSON. ONE tombstone anywhere in the batch therefore
// made the ENTIRE string ReceiveJSONMessages hands to 1C unparseable, not just
// that record. The same hole existed for a header whose value is absent
// (value_string() == nullptr, which is ordinary Kafka), for a present-but-empty
// message key, and for a present-but-empty header key.
//
// Absent now emits null and present-but-empty emits "", so the two stay
// distinguishable - which matters, because a tombstone means "this key was
// deleted" and an empty value means "this key holds nothing".
//
// Only the UNESCAPED path was ever broken: with the Escape* properties at their
// True defaults everything is written inside quotes and a tombstone has always
// come out as "". That asymmetry is deliberate and is asserted at the end.
//
// The records come from tools/kafka_null_producer - the component's own
// producer always passes librdkafka a non-NULL payload pointer and physically
// cannot write a tombstone.
void caseNullValues(ComponentLibrary& lib)
{
    const std::string topic = scratchTopic("nulls");

    phase("nulls: producing a tombstone and a null header value with librdkafka directly");
    {
        const std::string helper = executableDir() + "/kafka_null_producer";
        std::string       output;
        REQUIRE_MSG(fileExists(helper),
                    "the kafka_null_producer helper is missing at " + helper +
                        " - it is built by tests/CMakeLists.txt next to this binary, and without "
                        "it a tombstone cannot be produced at all. This is a FAILURE and not a "
                        "skip on purpose: the case would otherwise look green while proving "
                        "nothing.");
        const bool ok = runHelper(helper, {g_cfg.bootstrap, g_cfg.caPath, topic}, &output);
        REQUIRE_MSG(ok, "kafka_null_producer failed:\n" + clip(output, 2000));
        CHECK_CONTAINS(output, "ok 4");
        test::note("kafka_null_producer wrote 4 records to " + topic);
    }

    phase("nulls: reading them back with all four Escape* properties set to False");
    {
        const PoolRead read = readTopicPool(
            lib, "nulls", topic, 4,
            {{u"EscapeMessageValue", false},
             {u"EscapeMessageKey", false},
             {u"EscapeMessageHeaderValue", false},
             {u"EscapeMessageHeaderKey", false}},
            false);
        if (!read.reached) {
            return;
        }
        CHECK_EQ(read.error, std::string(kNoError));
        test::note("unescaped pool (" + std::to_string(read.pool.size()) + " bytes): " +
                   clip(read.pool, 700));

        // The exact byte sequences the defect produced. A parser is checked
        // below too, but these say WHICH hole reopened if one ever does.
        CHECK_MSG(read.pool.find("\"Value\":,") == std::string::npos,
                  "the pool still contains \"Value\": followed directly by a comma - an absent "
                  "message or header value is being written as nothing");
        CHECK_MSG(read.pool.find("\"Value\":}") == std::string::npos,
                  "the pool still contains \"Value\": followed directly by a closing brace");
        CHECK_MSG(read.pool.find("\"Key\":,") == std::string::npos,
                  "the pool still contains \"Key\": followed directly by a comma - a zero-length "
                  "key is being written as nothing");

        mini::Node parsed;
        bool       parseOk = true;
        try {
            parsed = mini::parse(read.pool);
        } catch (const std::exception& e) {
            parseOk = false;
            FAIL(std::string("the unescaped message pool is not valid JSON: ") + e.what() + "\n" +
                 clip(read.pool, 1200));
        }
        if (!parseOk) {
            return;
        }
        REQUIRE_MSG(parsed.isArray() && parsed.items.size() == 4,
                    "expected 4 records, got " + std::to_string(parsed.items.size()) + ": " +
                        clip(read.pool, 1200));

        // --- the tombstone --------------------------------------------------
        const mini::Node* tombstone = recordWithKey(parsed, "r1-tombstone");
        REQUIRE_MSG(tombstone != nullptr, "the tombstone record is missing from the pool");
        const mini::Node* tombValue = tombstone->member("Value");
        REQUIRE_MSG(tombValue != nullptr, "the tombstone record has no Value member at all");
        CHECK_MSG(tombValue->isNull(),
                  "a tombstone (value ABSENT) must come back as JSON null with escaping off");

        // --- present but empty, which must NOT read as a tombstone ----------
        const mini::Node* emptyValue = recordWithKey(parsed, "r2-emptyvalue");
        REQUIRE_MSG(emptyValue != nullptr, "the empty-value record is missing from the pool");
        const mini::Node* emptyNode = emptyValue->member("Value");
        REQUIRE_MSG(emptyNode != nullptr, "the empty-value record has no Value member");
        CHECK_MSG(emptyNode->isString() && emptyNode->text.empty(),
                  "a present but zero-length value must come back as \"\", not as null");
        // The whole point: the two are different in the document.
        CHECK_MSG(tombValue->isNull() && !emptyNode->isNull(),
                  "a deleted key and a key holding nothing are indistinguishable in the pool");

        // --- headers --------------------------------------------------------
        const mini::Node* hNull = headerWithKey(*tombstone, "h-null");
        REQUIRE_MSG(hNull != nullptr, "the header with an absent value is missing");
        const mini::Node* hNullValue = hNull->member("Value");
        REQUIRE_MSG(hNullValue != nullptr, "that header has no Value member");
        CHECK_MSG(hNullValue->isNull(), "a header whose value is ABSENT must come back as null");

        const mini::Node* hEmpty = headerWithKey(*tombstone, "h-empty");
        REQUIRE_MSG(hEmpty != nullptr, "the header with an empty value is missing");
        const mini::Node* hEmptyValue = hEmpty->member("Value");
        REQUIRE_MSG(hEmptyValue != nullptr, "that header has no Value member");
        CHECK_MSG(hEmptyValue->isString() && hEmptyValue->text.empty(),
                  "a header with a present but zero-length value must come back as \"\"");

        // A zero-length header KEY is legal in the Kafka protocol and had the
        // identical defect; it is not one of the three sites the review named.
        const mini::Node* hNoKey = headerWithKey(*tombstone, "");
        REQUIRE_MSG(hNoKey != nullptr, "the header with an EMPTY KEY is missing from the pool");
        CHECK_EQ(hNoKey->str("Value"), std::string("hv"));

        // --- empty and absent message keys ----------------------------------
        // Both come back as "": the key side has no null, only "".
        int emptyKeys = 0;
        for (const mini::Node& record : parsed.items) {
            const mini::Node* key = record.member("Key");
            CHECK_MSG(key != nullptr && key->isString(),
                      "every record must carry a string Key, never null and never nothing");
            if (key != nullptr && key->isString() && key->text.empty()) {
                ++emptyKeys;
            }
        }
        // R3 (present, zero length) and R4 (absent).
        CHECK_EQ(emptyKeys, 2);
    }

    phase("nulls: the same records with the Escape* properties at their defaults");
    {
        // The deliberate asymmetry. With escaping ON the builder writes inside
        // quotes, so a tombstone is flattened to "" and is indistinguishable
        // from an empty value. That path was never broken and was not changed;
        // it is pinned here so the difference stays a decision and not a
        // surprise.
        const PoolRead read = readTopicPool(lib, "nulls-escaped", topic, 4, {}, false);
        if (!read.reached) {
            return;
        }
        CHECK_EQ(read.error, std::string(kNoError));

        mini::Node parsed;
        try {
            parsed = mini::parse(read.pool);
        } catch (const std::exception& e) {
            FAIL(std::string("the escaped message pool is not valid JSON: ") + e.what() + "\n" +
                 clip(read.pool, 1200));
            return;
        }
        REQUIRE(parsed.isArray() && parsed.items.size() == 4);

        // With escaping on the keys arrive quoted-inside-quotes, so the lookup
        // key carries the quotes the producer put in the bytes.
        const mini::Node* tombstone = recordWithKey(parsed, "\"r1-tombstone\"");
        REQUIRE_MSG(tombstone != nullptr,
                    "the tombstone record is missing from the escaped pool: " +
                        clip(read.pool, 1200));
        const mini::Node* value = tombstone->member("Value");
        REQUIRE(value != nullptr);
        CHECK_MSG(value->isString() && value->text.empty(),
                  "with escaping ON a tombstone is expected to flatten to \"\" - if it is null "
                  "now, the escaped path has changed too and 1C scripts that read it will see "
                  "a different type");
    }
}

//---------------------------------------------------------------------------//
// Case 9 - a payload that is not valid UTF-8, read without base64
//---------------------------------------------------------------------------//

// ComponentBase::allocString() used to return void. When iconv refused the
// pool - which is exactly what happens the moment a Kafka payload carries bytes
// that are not valid UTF-8 and the script asked for ReceiveJSONMessages(False) -
// the tVariant was left as an empty VTYPE_PWSTR and nobody was told. The script
// saw an empty string and ErrorDescription == "Sucess".
//
// That is not merely a bad message. JSON_KafkaMessagePool DELETES every message
// it serialises when RemoveMessagesFromLocalQueueOnJSONBuild is set, and it does
// that BEFORE the string is handed over - so the whole poll was already gone by
// the time the conversion failed. A 1C loop of
// ConsumePool/ReceiveJSONMessages/repeat silently dropped every batch that
// contained one binary message, forever, and reported success each time.
//
// allocString now returns false with a reason, and the caller turns that into
// an ErrorDescription that also says how to get the data: base64 = True.
void caseNonUtf8Payload(ComponentLibrary& lib)
{
    const std::string topic = scratchTopic("badutf8");

    // FF FE FD: three bytes that cannot start a UTF-8 sequence. DecodeBase64Value
    // is the only way a 1C script can produce arbitrary bytes through this
    // component.
    phase("bad utf-8: producing FF FE FD through DecodeBase64Value");
    if (!produceBase64Value(lib, topic, "badutf8", "bad-utf8", "//79")) {
        return;
    }

    phase("bad utf-8: ReceiveJSONMessages(False) must report the failure");
    {
        const PoolRead read = readTopicPool(
            lib, "badutf8", topic, 1,
            {{u"RemoveMessagesFromLocalQueueOnJSONBuild", true}}, false);
        if (!read.reached) {
            return;
        }

        test::note("ReceiveJSONMessages(false) -> " + onec::varTypeName(read.variantType) +
                   ", " + std::to_string(read.pool.size()) + " byte(s); ErrorDescription = " +
                   read.error);

        // The variant itself is unchanged by the fix and is pinned so the
        // failure stays readable from 1C: an empty string, never a dangling
        // VTYPE_PWSTR.
        CHECK_MSG(read.pool.empty(),
                  "an ill-formed payload must not produce a partially converted pool");
        CHECK_MSG(static_cast<int>(read.variantType) == static_cast<int>(VTYPE_PWSTR) ||
                      static_cast<int>(read.variantType) == static_cast<int>(VTYPE_EMPTY),
                  "unexpected return type " + onec::varTypeName(read.variantType));

        // THE GUARD. On the old build this is "Sucess".
        CHECK_MSG(read.error != std::string(kNoError),
                  "the pool could not be converted and the call still reported success - a 1C "
                  "script cannot tell this from an empty poll, and with "
                  "RemoveMessagesFromLocalQueueOnJSONBuild set the batch is already gone");
        CHECK_CONTAINS(read.error, "cannot return the message pool to 1C");
        CHECK_CONTAINS(read.error, "not valid UTF-8");
        // The text has to say what to do about it, or the script has no way out.
        CHECK_CONTAINS(read.error, "base64");

        // The data loss the message is about, pinned: the batch really is gone.
        CHECK_MSG(read.lengthBefore >= 1,
                  "nothing was in the local queue, the case tested nothing");
        CHECK_EQ(read.lengthAfter, 0L);
    }

    phase("bad utf-8: the same batch read with base64 = True comes back intact");
    {
        // The advice in the error message has to actually work, otherwise the
        // script is told to do something that does not help. A fresh group
        // re-reads the same records from the beginning.
        const PoolRead read = readTopicPool(lib, "badutf8-b64", topic, 1, {}, true);
        if (!read.reached) {
            return;
        }
        CHECK_EQ(read.error, std::string(kNoError));

        mini::Node parsed;
        try {
            parsed = mini::parse(read.pool);
        } catch (const std::exception& e) {
            FAIL(std::string("the base64 pool is not valid JSON: ") + e.what() + "\n" +
                 clip(read.pool, 600));
            return;
        }
        REQUIRE(parsed.isArray() && parsed.items.size() == 1);
        // base64encode = True encodes the KEY as well as the value, so the
        // ASCII key "bad-utf8" arrives as its base64 form. Pinned rather than
        // worked around: a script that flips the parameter to rescue a binary
        // batch has to decode the keys too, and nothing else in the suite says
        // so.
        CHECK_EQ(parsed.items[0].str("Key"), std::string("YmFkLXV0Zjg="));
        CHECK_MSG(parsed.items[0].str("Value") == std::string("//79"),
                  "the base64 form of FF FE FD did not survive: \"" +
                      parsed.items[0].str("Value") + "\"");
    }
}

//---------------------------------------------------------------------------//
// Case 10 - a 0x00 byte inside a payload, read without escaping
//---------------------------------------------------------------------------//

// allocString() used to set
//     pvarPropVal->wstrLen = strlen16(pvarPropVal->pwstrVal);
// i.e. the distance to the first NUL in the buffer it had just filled. tVariant
// ::wstrLen is documented as a COUNT OF CHARACTERS (include/types.h:153), and a
// raw 0x00 byte in a Kafka payload becomes a real U+0000 inside the string when
// EscapeMessageValue is off - so the platform was handed everything BEFORE that
// character and the rest was silently dropped, while the allocation was still
// holding it. A 1C script got a JSON document cut mid-record and no error.
//
// wstrLen is now what the converter reports it produced.
//
// What this case can and cannot prove is worth being precise about: it is
// asserted against the TEST HARNESS, which reads a returned string as
// utf16To8(pwstrVal, wstrLen) exactly as the documented contract says. There is
// no 1C platform on this machine, so "the platform shows the whole string" is
// not something the suite can observe - only "the component now describes the
// whole string instead of describing a prefix of it".
void caseEmbeddedNulByte(ComponentLibrary& lib)
{
    const std::string topic = scratchTopic("nulbyte");

    // QQBC == 41 00 42 == 'A', U+0000, 'B'.
    phase("embedded NUL: producing 41 00 42 through DecodeBase64Value");
    if (!produceBase64Value(lib, topic, "nulbyte", "nul-byte", "QQBC")) {
        return;
    }

    phase("embedded NUL: with EscapeMessageValue = False the byte must survive");
    {
        const PoolRead read =
            readTopicPool(lib, "nulbyte", topic, 1, {{u"EscapeMessageValue", false}}, false);
        if (!read.reached) {
            return;
        }
        CHECK_EQ(read.error, std::string(kNoError));

        // Two lengths: what the component SAID it produced (wstrLen, which is
        // what the harness uses to build this std::string, exactly as the
        // documented contract says the platform does) and the distance to the
        // first NUL inside it, which is what the old code reported instead.
        const std::size_t firstNul   = read.pool.find('\0');
        const std::size_t reported   = read.pool.size();
        const std::size_t toFirstNul = std::strlen(read.pool.c_str());
        test::note("unescaped pool: " + std::to_string(reported) +
                   " byte(s) returned, first NUL at " +
                   (firstNul == std::string::npos ? std::string("(none)")
                                                  : std::to_string(firstNul)));

        // Deliberately no REQUIRE below: on a build with the defect every one
        // of these says something different about the same loss, and all of
        // them are worth seeing in the report at once.

        // THE GUARD, stated as a number: the reported length has to be longer
        // than the distance to the first NUL. On the old build the two were
        // equal, because that distance IS what it reported.
        CHECK_MSG(reported > toFirstNul,
                  "wstrLen still measures to the first NUL (" + std::to_string(toFirstNul) +
                      " of " + std::to_string(reported) +
                      " bytes) rather than counting what the converter produced");
        CHECK_MSG(firstNul != std::string::npos,
                  "the 0x00 byte is not in the returned string at all - the pool was cut at it: " +
                      clip(read.pool, 600));
        CHECK_MSG(firstNul != std::string::npos && reported > firstNul + 1,
                  "the returned string ends at the 0x00 byte; everything the payload carried "
                  "after it was dropped even though the allocation still held it");

        // The three payload bytes, in order, with the NUL between them.
        const std::string wanted("\"Value\":A\0B", 11);
        CHECK_MSG(read.pool.find(wanted) != std::string::npos,
                  "the payload did not come back as A<NUL>B: " + clip(read.pool, 600));

        // And the record is complete to its last byte.
        CHECK_MSG(reported >= 2 && read.pool.compare(reported - 2, 2, "}]") == 0,
                  "the pool does not end with the closing of the record array: " +
                      clip(read.pool, 600));
        CHECK_CONTAINS(read.pool, "\"Headers\":[]}]");
    }

    phase("embedded NUL: with escaping ON the byte is escaped and the pool parses");
    {
        // The default path, unchanged by the fix: escape_string_simple writes
        //   and the document stays ordinary text.
        const PoolRead read = readTopicPool(lib, "nulbyte-escaped", topic, 1, {}, false);
        if (!read.reached) {
            return;
        }
        CHECK_EQ(read.error, std::string(kNoError));
        CHECK_MSG(read.pool.find('\0') == std::string::npos,
                  "the escaped pool contains a raw 0x00 byte");
        CHECK_CONTAINS(read.pool, "\\u0000");

        mini::Node parsed;
        try {
            parsed = mini::parse(read.pool);
        } catch (const std::exception& e) {
            FAIL(std::string("the escaped pool is not valid JSON: ") + e.what() + "\n" +
                 clip(read.pool, 600));
            return;
        }
        REQUIRE(parsed.isArray() && parsed.items.size() == 1);
        CHECK_EQ(parsed.items[0].str("Key"), std::string("nul-byte"));
        CHECK_EQ(parsed.items[0].str("Value"), std::string("A\0B", 3));
    }
}

//---------------------------------------------------------------------------//
// Preflight
//---------------------------------------------------------------------------//

void printBanner()
{
    // Never "Kafka 4.x" on trust: if nobody told us the version, the banner
    // says so, so no --external-broker run can be quoted as 4.x evidence.
    const std::string broker = g_cfg.brokerVersion.empty()
                                   ? std::string("a broker of UNVERIFIED version")
                                   : ("Kafka " + g_cfg.brokerVersion);

    std::cout << "=============================================================" << std::endl;
    std::cout << "onec-librdkafka TLS test against " << broker << std::endl;
    std::cout << "  component : " << g_cfg.soPath << std::endl;
    std::cout << "  bootstrap : " << g_cfg.bootstrap << std::endl;
    std::cout << "  ca        : " << g_cfg.caPath << std::endl;
    std::cout << "  other ca  : " << g_cfg.otherCaPath << std::endl;
    std::cout << "  broker    : "
              << (g_cfg.brokerVersion.empty() ? std::string("UNVERIFIED (KAFKA_TEST_BROKER_VERSION "
                                                            "is not set)")
                                              : g_cfg.brokerVersion)
              << ", expected major " << g_cfg.expectBrokerMajor << std::endl;
    std::cout << "  topic     : " << g_cfg.topic << std::endl;
    std::cout << "  messages  : " << g_cfg.messageCount << std::endl;
    std::cout << "  run tag   : " << g_cfg.runTag << std::endl;
    std::cout << "  timeouts  : produce " << g_cfg.produceTimeoutMs << " ms, consume "
              << g_cfg.consumeTimeoutMs << " ms, admin " << g_cfg.adminTimeoutMs
              << " ms, negative " << g_cfg.negativeTimeoutMs << " ms, hard "
              << g_cfg.hardTimeoutMs << " ms" << std::endl;
    std::cout << "=============================================================" << std::endl;
}

// Returns kExitOk when the run may proceed, otherwise the exit code to use.
int preflight()
{
    if (!fileExists(g_cfg.caPath)) {
        std::cout << (g_cfg.requireBroker ? "FAIL: " : "SKIP: ") << "the CA certificate '"
                  << g_cfg.caPath << "' does not exist." << std::endl;
        std::cout << "      Run tests/docker/up.sh first, or point KAFKA_SSL_CA at the broker CA."
                  << std::endl;
        return g_cfg.requireBroker ? kExitFailed : kExitSkipped;
    }

    std::string why;
    if (!tcpReachable(g_cfg.bootstrap, g_cfg.probeTimeoutMs, &why)) {
        // KAFKA_TEST_REQUIRE_BROKER is for the runs that must not go quiet: a
        // skip is the right answer on a developer box with no docker, and the
        // wrong answer where the broker is supposed to be up.
        if (g_cfg.requireBroker) {
            std::cout << "FAIL: no Kafka broker listening on " << g_cfg.bootstrap << " (" << why
                      << ")." << std::endl;
            std::cout << "      KAFKA_TEST_REQUIRE_BROKER=1, so an unreachable broker is a failure, "
                         "not a skip."
                      << std::endl;
            return kExitFailed;
        }
        std::cout << "SKIP: no Kafka broker listening on " << g_cfg.bootstrap << " (" << why << ")."
                  << std::endl;
        std::cout << "      Start it with tests/docker/up.sh, or set KAFKA_SSL_BOOTSTRAP."
                  << std::endl;
        std::cout << "      Nothing was tested - this is a skip, not a pass." << std::endl;
        std::cout << "      Set KAFKA_TEST_REQUIRE_BROKER=1 to make this a failure instead."
                  << std::endl;
        return kExitSkipped;
    }
    return kExitOk;
}

} // namespace

//---------------------------------------------------------------------------//

int main(int argc, char** argv)
{
    g_cfg = buildConfig(argc, argv);
    printBanner();

    const int skip = preflight();
    if (skip != kExitOk) {
        return skip;
    }

    Watchdog watchdog(std::chrono::milliseconds(g_cfg.hardTimeoutMs));

    std::unique_ptr<ComponentLibrary> library;
    try {
        phase("loading " + g_cfg.soPath);
        library.reset(new ComponentLibrary(g_cfg.soPath));
    } catch (const HostError& e) {
        std::cerr << "SETUP FAILURE: cannot load the component: " << e.what() << std::endl;
        std::cerr << "               build it first (cmake --build out64), or pass the .so path as "
                     "argv[1] / $ONEC_KAFKA_SO."
                  << std::endl;
        return kExitSetup;
    }

    // librdkafka leaves background threads and OpenSSL state behind; unmapping
    // the library underneath them at exit buys nothing and can turn a green run
    // into a segfault after the last check. The process is about to end anyway.
    library->setCloseOnDestroy(false);
    ComponentLibrary& lib = *library;

    test::run("the broker really is the version this run claims", [] { caseBrokerVersion(); });
    test::run("negative controls: the bootstrap port only speaks TLS, and the chain is verified",
              [&lib] { caseNegativeControls(lib); });
    test::run("producer: " + std::to_string(g_cfg.messageCount) + " messages delivered over TLS",
              [&lib] { caseProduce(lib); });
    test::run("consumer: the same messages come back over TLS, byte for byte",
              [&lib] { caseConsume(lib); });
    test::run("librdkafka reports a completed, verified TLS handshake and never plaintext",
              [&lib] { caseTransportEvidence(lib); });
    test::run("3-byte and 4-byte UTF-8 survive toUTF8String and the round trip",
              [&lib] { caseThreeByteUtf8(lib); });
    test::run("C0 control bytes are escaped as \\u001f and the pool stays valid JSON",
              [&lib] { caseControlCharacters(lib); });
    test::run("a tombstone and a null header value keep the unescaped pool parseable",
              [&lib] { caseNullValues(lib); });
    test::run("a payload that is not valid UTF-8 is reported, not returned as an empty string",
              [&lib] { caseNonUtf8Payload(lib); });
    test::run("a 0x00 byte in a payload is not silently truncated away",
              [&lib] { caseEmbeddedNulByte(lib); });

    phase("done");
    return test::summary("kafka-ssl") == 0 ? kExitOk : kExitFailed;
}
