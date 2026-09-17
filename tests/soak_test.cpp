// Soak / leak test for librdkafka_onec.so.
//
// The component is going to move millions of messages inside a long-lived 1C
// process. At that scale the interesting question is not "does it work once"
// but "does it still fit in memory after the millionth message". Three distinct
// failure modes matter, and no single tool sees all three:
//
//   1. a classic leak - new without delete, AllocMemory without FreeMemory.
//      ASan/LSan catches it; this binary built with -fsanitize=address does too.
//   2. UNBOUNDED CONTAINER GROWTH - the consumer keeps every consumed message in
//      a local pool (KafkaConsumerCore1C::local_queue, src/consumer1c.h:63). The
//      memory stays reachable, so ASan says nothing at all, yet RSS climbs
//      linearly and the 1C process eventually dies. THIS is the mode this test
//      exists for.
//   3. data races - librdkafka calls back into the component from its own
//      threads. TSan territory; build with -fsanitize=thread and run this.
//
// How mode 2 is detected
// ----------------------
// The test drives a real produce -> consume -> ReceiveJSONMessages ->
// ClearMessagePool cycle through the component, exactly the way
// tests/kafka_ssl_test.cpp does, for as many messages as it is told to, and
// samples five numbers along the way:
//
//   * RSS, from /proc/self/statm (resident pages x page size)
//   * the component's own pool length, GetMessagePoolLength()
//   * the harness memory manager's liveBlocks() - every string the component
//     hands back comes from it, and it never calls FreeMemory itself
//   * the number of open file descriptors, from /proc/self/fd - an fd leak at
//     this scale is just as fatal as a memory leak
//   * wall clock, so the table doubles as a throughput log
//
// The first samples are thrown away: librdkafka's per-broker send/receive
// buffers, the glibc arena and the JSON pool all grow legitimately at the start
// of a run. Over the remaining samples the test fits a least-squares line of
// RSS against messages and reads the slope in BYTES PER MESSAGE. That is the
// right statistic, because a plateau - even a high one - is harmless, while any
// non-zero slope multiplied by a production message count is not.
//
// The threshold, and why it cannot fire on noise
// ----------------------------------------------
// The floor under both rules below is maxBytesPerMessage = 32 B/message. There
// is no legitimate per-message growth in steady state: librdkafka's
// per-partition state is O(partitions), not O(messages), and the component is
// supposed to drop each batch in ClearMessagePool. Every realistic instance of
// mode 2 is far bigger: a retained RdKafka::Message costs the payload plus the
// key plus the headers plus the object itself, i.e. hundreds of bytes for the
// 256-byte payload used here. 32 B/message is also 32 MB per million - a third
// of the "100 bytes per message is 100 MB per million" figure this test was
// written for, so it trips well before production notices.
//
// A run fails when EITHER rule fires:
//
//   A. magnitude   slope > 32 B/msg + rssNoiseBytes / steadyStateMessages
//                  (rssNoiseBytes = 16 MiB by default)
//
//      The added term is the anti-flake. RSS is quantised to 4 KiB pages and
//      glibc returns freed memory to the OS only at its own trim points, so the
//      curve is a staircase, not a line, and a short window can show a steep
//      slope from a single step. Amortising a fixed allowance over the window
//      makes the bar automatically unreachable on a short run (20k messages ->
//      800 B/message of allowance alone) and automatically irrelevant on a long
//      one (1M messages -> 16 B/message on top of the floor). A fixed "fail
//      above N megabytes" bar would do the opposite: flaky when short, blind
//      when long.
//
//   B. linearity   slope > max(32 B/msg, minLinearGrowth / steadyStateMessages)
//                  AND Pearson r >= 0.98
//                  AND the measured end-to-end growth exceeds minLinearGrowth
//                  (minLinearGrowth = 2 MiB by default)
//
//      Rule A alone is blind to a small leak on a short run - a real 512
//      B/message leak measured at 514 B/message with r = 1.000 sailed under an
//      578 B/message allowance during development, which is exactly the kind of
//      false negative that makes a soak test worthless. Rule B is what catches
//      it: an allocator staircase is not a straight line, so requiring r >= 0.98
//      across dozens of samples separates "the heap stepped once" from "every
//      message costs us bytes". Observed r on healthy runs of this component was
//      -0.086 (200k messages) and -0.577 (5k messages); a genuine leak reads
//      1.000. The 2 MiB minimum keeps page quantisation from manufacturing a
//      line out of nothing.
//
// Together the two rules give this run's detection floor, which the verdict
// prints so nobody has to guess what a green run actually ruled out.
//
// Both rules are switched off on a sanitizer build and under valgrind, where
// RSS belongs to the tool rather than to the component - see the comment above
// ONEC_SOAK_SANITIZER for the measurements. A run there still asserts the pool,
// liveBlocks(), the fd count and the round trip, and LSan/TSan report for
// themselves; it simply does not claim to have ruled out unbounded growth.
//
// The same shape guards the file descriptors: the count after warm-up may not
// grow by more than KAFKA_SOAK_MAX_FD_GROWTH (default 8) for the rest of the run.
//
// Everything else is asserted directly rather than inferred: the pool length is
// 0 after every ClearMessagePool, liveBlocks() is 0 at every sample and at the
// end, and every message produced comes back.
//
// Configuration (all optional):
//   KAFKA_SSL_BOOTSTRAP           localhost:9093
//   KAFKA_SSL_CA                  tests/docker/secrets/ca.pem (searched upwards)
//   KAFKA_TEST_TOPIC              onec-librdkafka-soak-test (a per-run suffix is
//                                 appended, so no run ever sees another's data)
//   KAFKA_SOAK_TOPIC              exact topic name, suppresses the suffix
//   KAFKA_TEST_REQUIRE_BROKER     1 = an unreachable broker is a FAILURE, not a skip
//   KAFKA_SOAK_MESSAGES           1000000
//   KAFKA_SOAK_PAYLOAD_BYTES      256
//   KAFKA_SOAK_BATCH              1000     (messages per produce/consume cycle)
//   KAFKA_SOAK_POLL_MS            1000     (one ConsumePool call)
//   KAFKA_SOAK_SAMPLE_EVERY       messages between samples (default: ~50 samples)
//   KAFKA_SOAK_WARMUP_PCT         25       (share of samples discarded up front)
//   KAFKA_SOAK_MAX_BYTES_PER_MSG  32
//   KAFKA_SOAK_RSS_NOISE_MB       16
//   KAFKA_SOAK_MIN_LINEAR_MB      2        (rule B, see above)
//   KAFKA_SOAK_MAX_FD_GROWTH      8
//   KAFKA_SOAK_MAX_SECONDS        0        (0 = no wall-clock budget)
//   KAFKA_SOAK_SELF_TEST_BYTES    0        (see --self-test)
//   KAFKA_SOAK_IGNORE_RSS         1 = do not apply the RSS growth rule at all
//   KAFKA_SOAK_PRODUCE_TIMEOUT_MS 30000    (message.timeout.ms)
//   KAFKA_SOAK_BATCH_TIMEOUT_MS   30000    (deadline for consuming one batch)
//   KAFKA_SOAK_WARMUP_TIMEOUT_MS  60000    (deadline for the very first batch)
//   KAFKA_TEST_HARD_TIMEOUT_MS    60000 + 3 ms per message (watchdog)
//   ONEC_KAFKA_SO                 path to librdkafka_onec.so (argv wins)
//
// Command line (overrides the environment):
//   soak_test [/path/to/librdkafka_onec.so] [options]
//     --quick              20000 messages, batch 500 - a run you can wait for
//     --messages=N         total messages to push through
//     --payload=N          payload bytes per message
//     --batch=N            messages per produce/consume cycle
//     --poll-ms=N          ConsumePool timeout
//     --sample-every=N     messages between samples
//     --max-seconds=N      stop early after N seconds and analyse what was done
//     --self-test=N        retain N bytes per message inside the TEST process and
//                          invert the verdict: the run passes only if the RSS
//                          detector fires. A growth threshold that has never been
//                          seen to trip is decoration, so this is how the suite
//                          proves its own leak detector still works:
//                            soak_test --quick --self-test=512   must pass
//                            soak_test --quick                   must also pass
//     --so=PATH            the component to load
//     -h, --help
//
// Exit codes (the same contract as kafka_ssl_test):
//   0   every check passed
//   1   at least one check failed
//   2   setup problem - the component could not be loaded
//   3   hard timeout, the run was killed by the watchdog
//   77  skipped - the broker is not reachable / the CA is missing
//       (never returned when KAFKA_TEST_REQUIRE_BROKER=1)

#include "host/component_loader.h"
#include "host/test_assert.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <condition_variable>
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
#include <vector>

#include <dirent.h>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

//---------------------------------------------------------------------------//
// Is RSS a number we are allowed to reason about?
//---------------------------------------------------------------------------//

// Under a sanitizer or under valgrind it is not, and this is not a detail.
// Measured on this component, 20000 messages, 500 per batch:
//
//   plain build                    RSS flat at 18.8 MiB, slope 0.00 B/message
//   -fsanitize=address (defaults)  RSS 40 -> 328 MiB, slope 15335 B/message
//   -fsanitize=address, 8 MiB quarantine   slope 138 B/message
//
// That is ASan's redzones and its 256 MiB free-quarantine filling up, not the
// component; the same run without ASan is flat as a board. A soak that failed
// on that number would be pure noise - and at the default quarantine it would
// fail every single time, which is how a test gets switched off for good.
//
// So the slope rule is suppressed on such builds and says loudly that it was.
// Everything else the test asserts - the pool draining to zero, liveBlocks()
// staying at zero, the fd count, every message coming back - is still perfectly
// valid there, and LSan's own report is the whole point of that run.
#if defined(__SANITIZE_ADDRESS__)
#  define ONEC_SOAK_SANITIZER "address"
#elif defined(__SANITIZE_THREAD__)
#  define ONEC_SOAK_SANITIZER "thread"
#elif defined(__has_feature)
#  if __has_feature(address_sanitizer)
#    define ONEC_SOAK_SANITIZER "address"
#  elif __has_feature(thread_sanitizer)
#    define ONEC_SOAK_SANITIZER "thread"
#  elif __has_feature(memory_sanitizer)
#    define ONEC_SOAK_SANITIZER "memory"
#  endif
#endif

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

const int kExitOk      = 0;
const int kExitFailed  = 1;
const int kExitSetup   = 2;
const int kExitTimeout = 3;
const int kExitSkipped = 77;

// Fewer than this many steady-state samples is not a line, it is two dots and
// an opinion. The sampling interval is derived from the message count so that
// even --quick produces ~50 samples, which makes this unreachable in practice;
// it is here so that a hand-set KAFKA_SOAK_SAMPLE_EVERY cannot quietly turn the
// slope test into a coin flip.
const std::size_t kMinSteadySamples = 6;

// Rule B's linearity bar. A leak that costs the same on every message draws a
// straight line through the samples; an allocator staircase, a one-off buffer
// or page-quantisation noise does not. Measured on this component: r = -0.086
// over 200k messages and -0.577 over 5k messages when healthy, r = 1.000 with a
// deliberate per-message leak. 0.98 sits in the enormous gap between the two.
const double kLinearR = 0.98;

//---------------------------------------------------------------------------//
// Progress
//---------------------------------------------------------------------------//

// Shared with the watchdog thread, so a hard timeout can say what the process
// was doing when it stopped making progress.
std::mutex              g_phaseMutex;
std::string             g_phase = "startup";
const Clock::time_point g_started = Clock::now();

double elapsedSeconds()
{
    return std::chrono::duration<double>(Clock::now() - g_started).count();
}

std::string stamp()
{
    std::ostringstream out;
    out << "t+" << std::fixed << std::setprecision(1) << std::setw(7) << elapsedSeconds() << "s";
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

std::string clip(const std::string& text, std::size_t limit = 400)
{
    if (text.size() <= limit) {
        return text;
    }
    return text.substr(0, limit) + "... (" + std::to_string(text.size()) + " bytes total)";
}

//---------------------------------------------------------------------------//
// Watchdog
//---------------------------------------------------------------------------//

// Identical in spirit to the one in kafka_ssl_test: by the time it fires the
// main thread is stuck inside librdkafka, so it prints and calls _Exit rather
// than trying to unwind.
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
        std::cerr << "         a soak test that stops making progress is a failure, not a slow pass; "
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
// Process metrics
//---------------------------------------------------------------------------//

// /proc/self/statm field 2 is the resident set in pages. It is the number the
// OOM killer and the 1C administrator both look at, which is why the test
// measures it rather than anything malloc reports about itself.
long long rssBytes()
{
    std::ifstream statm("/proc/self/statm");
    if (!statm) {
        return -1;
    }
    long long totalPages = 0;
    long long residentPages = 0;
    if (!(statm >> totalPages >> residentPages)) {
        return -1;
    }
    const long pageSize = ::sysconf(_SC_PAGESIZE);
    if (pageSize <= 0) {
        return -1;
    }
    return residentPages * static_cast<long long>(pageSize);
}

// Entries in /proc/self/fd, minus ".", ".." and the descriptor opendir itself
// is holding while we count.
long openFdCount()
{
    DIR* dir = ::opendir("/proc/self/fd");
    if (dir == nullptr) {
        return -1;
    }
    long count = 0;
    while (const dirent* entry = ::readdir(dir)) {
        if (std::strcmp(entry->d_name, ".") == 0 || std::strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        ++count;
    }
    ::closedir(dir);
    return count > 0 ? count - 1 : count;
}

// valgrind reports its own resident set through /proc/self/statm, including all
// of memcheck's shadow state, so RSS there grows for reasons that have nothing
// to do with the component. There is no libc call for "am I under valgrind"
// without valgrind.h, but its preload library is always in the maps.
bool runningUnderValgrind()
{
    std::ifstream maps("/proc/self/maps");
    if (!maps) {
        return false;
    }
    std::string line;
    while (std::getline(maps, line)) {
        if (line.find("vgpreload") != std::string::npos) {
            return true;
        }
    }
    return false;
}

std::string humanBytes(long long bytes)
{
    const char* const units[] = {"B", "KiB", "MiB", "GiB"};
    double            value = static_cast<double>(bytes);
    int               unit = 0;
    const bool        negative = value < 0;
    if (negative) {
        value = -value;
    }
    while (value >= 1024.0 && unit < 3) {
        value /= 1024.0;
        ++unit;
    }
    std::ostringstream out;
    out << (negative ? "-" : "") << std::fixed << std::setprecision(unit == 0 ? 0 : 1) << value << ' '
        << units[unit];
    return out.str();
}

std::string signedBytes(long long bytes)
{
    return (bytes >= 0 ? "+" : "") + humanBytes(bytes);
}

//---------------------------------------------------------------------------//
// Small filesystem / environment helpers
//---------------------------------------------------------------------------//

bool fileExists(const std::string& path)
{
    std::ifstream file(path, std::ios::binary);
    return file.good();
}

std::string envOr(const char* name, const std::string& fallback)
{
    const char* value = std::getenv(name);
    return (value != nullptr && value[0] != '\0') ? std::string(value) : fallback;
}

bool envFlag(const char* name)
{
    const std::string value = envOr(name, "0");
    return value == "1" || value == "true" || value == "yes" || value == "on";
}

// Accepts values >= floor only; anything else is reported and ignored, because
// a typo in a soak knob must not silently turn the run into a no-op.
long long envNum(const char* name, long long fallback, long long floor)
{
    const char* value = std::getenv(name);
    if (value == nullptr || value[0] == '\0') {
        return fallback;
    }
    try {
        const long long parsed = std::stoll(value);
        if (parsed >= floor) {
            return parsed;
        }
    } catch (const std::exception&) {
        // fall through to the warning
    }
    std::cerr << "warning: " << name << "='" << value << "' is not an integer >= " << floor
              << ", using " << fallback << std::endl;
    return fallback;
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

//---------------------------------------------------------------------------//
// Is the broker even there?
//---------------------------------------------------------------------------//

// A plain TCP connect to the first address in the bootstrap list - the same
// probe kafka_ssl_test uses. It says nothing about TLS, only whether to run or
// to stop with a readable reason.
bool tcpReachable(const std::string& bootstrap, int timeoutMs, std::string* detail)
{
    std::string      host = bootstrap;
    const std::size_t comma = host.find(',');
    if (comma != std::string::npos) {
        host.erase(comma);
    }
    std::string       port = "9093";
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
    std::string topic;
    std::string runTag;
    std::string soPath;

    long long messages       = 1000000;
    long long payloadBytes   = 256;
    long long batch          = 1000;
    long long sampleEvery    = 0;        // filled in by finish()
    long long warmupPercent  = 25;
    long long maxBytesPerMsg = 32;
    long long rssNoiseBytes  = 16LL * 1024 * 1024;
    long long minLinearBytes = 2LL * 1024 * 1024;
    long long maxFdGrowth    = 8;
    long long maxSeconds     = 0;
    long long selfTestBytes  = 0;
    long long batchClampedFrom = 0;   // non-zero when --batch was reduced to fit the sampling

    int pollMs             = 1000;
    int produceTimeoutMs   = 30000;
    int batchTimeoutMs     = 30000;
    int warmupTimeoutMs    = 60000;
    int hardTimeoutMs      = 0;          // filled in by finish()
    int probeTimeoutMs     = 3000;

    bool requireBroker = false;
    bool explicitTopic = false;
    bool hardTimeoutSet = false;

    // Why RSS cannot be trusted on this run; empty when it can.
    std::string rssUntrustworthy;

    std::string group() const { return "onec-librdkafka-soak-" + runTag; }
    std::string client(const std::string& suffix) const
    {
        return "onec-librdkafka-soak-" + runTag + "-" + suffix;
    }
};

// The same search kafka_ssl_test does, so the binary works from the repository
// root or from tests/.
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

void usage(std::ostream& out, const char* argv0)
{
    out << "Usage: " << argv0 << " [/path/to/librdkafka_onec.so] [options]\n"
        << "\n"
        << "  --quick             20000 messages, batch 500 (a run you can wait for)\n"
        << "  --messages=N        total messages to push through the component\n"
        << "  --payload=N         payload bytes per message\n"
        << "  --batch=N           messages per produce/consume cycle\n"
        << "  --poll-ms=N         ConsumePool timeout, milliseconds\n"
        << "  --sample-every=N    messages between RSS/fd samples\n"
        << "  --max-seconds=N     stop early after N seconds and analyse what was done\n"
        << "  --self-test=N       retain N bytes per message inside the test process and\n"
        << "                      invert the verdict: the run passes only if the RSS\n"
        << "                      growth detector fires (proof the threshold works)\n"
        << "  --so=PATH           the component to load\n"
        << "  -h, --help          this text\n"
        << "\n"
        << "Every option also has a KAFKA_SOAK_* environment variable; see the header\n"
        << "comment of tests/soak_test.cpp.\n";
}

// Returns false when the program should stop (help, or a bad option).
bool parseArgs(int argc, char** argv, Config& cfg, int& exitCode)
{
    auto number = [&](const std::string& text, long long floor, long long& out) {
        try {
            const long long parsed = std::stoll(text);
            if (parsed < floor) {
                return false;
            }
            out = parsed;
            return true;
        } catch (const std::exception&) {
            return false;
        }
    };

    for (int i = 1; i < argc; ++i) {
        const std::string arg = (argv[i] != nullptr) ? argv[i] : "";
        if (arg.empty()) {
            continue;
        }

        if (arg == "-h" || arg == "--help") {
            usage(std::cout, argv[0]);
            exitCode = kExitOk;
            return false;
        }
        if (arg == "--quick") {
            cfg.messages = 20000;
            cfg.batch = 500;
            continue;
        }
        if (arg.rfind("--", 0) != 0) {
            cfg.soPath = arg;          // the positional .so path, as in the other tests
            continue;
        }

        const std::size_t eq = arg.find('=');
        if (eq == std::string::npos) {
            std::cerr << "unknown option: " << arg << "\n\n";
            usage(std::cerr, argv[0]);
            exitCode = kExitSetup;
            return false;
        }
        const std::string key = arg.substr(0, eq);
        const std::string value = arg.substr(eq + 1);

        bool ok = true;
        if (key == "--so") {
            cfg.soPath = value;
        } else if (key == "--messages") {
            ok = number(value, 1, cfg.messages);
        } else if (key == "--payload") {
            ok = number(value, 1, cfg.payloadBytes);
        } else if (key == "--batch") {
            ok = number(value, 1, cfg.batch);
        } else if (key == "--sample-every") {
            ok = number(value, 1, cfg.sampleEvery);
        } else if (key == "--max-seconds") {
            ok = number(value, 0, cfg.maxSeconds);
        } else if (key == "--self-test") {
            ok = number(value, 0, cfg.selfTestBytes);
        } else if (key == "--poll-ms") {
            long long ms = cfg.pollMs;
            ok = number(value, 1, ms);
            cfg.pollMs = static_cast<int>(ms);
        } else {
            std::cerr << "unknown option: " << key << "\n\n";
            usage(std::cerr, argv[0]);
            exitCode = kExitSetup;
            return false;
        }

        if (!ok) {
            std::cerr << "bad value for " << key << ": '" << value << "'" << std::endl;
            exitCode = kExitSetup;
            return false;
        }
    }
    return true;
}

Config buildConfig(int argc, char** argv, bool& proceed, int& exitCode)
{
    Config cfg;
    cfg.bootstrap     = envOr("KAFKA_SSL_BOOTSTRAP", "localhost:9093");
    cfg.caPath        = envOr("KAFKA_SSL_CA", defaultCaPath());
    cfg.requireBroker = envFlag("KAFKA_TEST_REQUIRE_BROKER");

    cfg.messages         = envNum("KAFKA_SOAK_MESSAGES", 1000000, 1);
    cfg.payloadBytes     = envNum("KAFKA_SOAK_PAYLOAD_BYTES", 256, 1);
    cfg.batch            = envNum("KAFKA_SOAK_BATCH", 1000, 1);
    cfg.sampleEvery      = envNum("KAFKA_SOAK_SAMPLE_EVERY", 0, 0);
    cfg.warmupPercent    = envNum("KAFKA_SOAK_WARMUP_PCT", 25, 0);
    cfg.maxBytesPerMsg   = envNum("KAFKA_SOAK_MAX_BYTES_PER_MSG", 32, 0);
    cfg.rssNoiseBytes    = envNum("KAFKA_SOAK_RSS_NOISE_MB", 16, 0) * 1024 * 1024;
    cfg.minLinearBytes   = envNum("KAFKA_SOAK_MIN_LINEAR_MB", 2, 0) * 1024 * 1024;
    cfg.maxFdGrowth      = envNum("KAFKA_SOAK_MAX_FD_GROWTH", 8, 0);
    cfg.maxSeconds       = envNum("KAFKA_SOAK_MAX_SECONDS", 0, 0);
    cfg.selfTestBytes    = envNum("KAFKA_SOAK_SELF_TEST_BYTES", 0, 0);
    cfg.pollMs           = static_cast<int>(envNum("KAFKA_SOAK_POLL_MS", 1000, 1));
    cfg.produceTimeoutMs = static_cast<int>(envNum("KAFKA_SOAK_PRODUCE_TIMEOUT_MS", 30000, 1000));
    cfg.batchTimeoutMs   = static_cast<int>(envNum("KAFKA_SOAK_BATCH_TIMEOUT_MS", 30000, 1000));
    cfg.warmupTimeoutMs  = static_cast<int>(envNum("KAFKA_SOAK_WARMUP_TIMEOUT_MS", 60000, 1000));

    const long long hardFromEnv = envNum("KAFKA_TEST_HARD_TIMEOUT_MS", 0, 0);
    cfg.hardTimeoutSet = hardFromEnv > 0;
    if (cfg.hardTimeoutSet) {
        cfg.hardTimeoutMs = static_cast<int>(std::min<long long>(hardFromEnv, 2147483647LL));
    }

    cfg.soPath = ComponentLibrary::defaultPath();

    proceed = parseArgs(argc, argv, cfg, exitCode);
    if (!proceed) {
        return cfg;
    }

    // Unique per run. The soak writes far too much data to share a topic with
    // anybody: a leftover backlog would be indistinguishable from this run's
    // messages and would make the pool-length assertions meaningless.
    const long long pid = static_cast<long long>(::getpid());
    const long long tick =
        std::chrono::duration_cast<std::chrono::microseconds>(Clock::now().time_since_epoch()).count();
    std::ostringstream tag;
    tag << pid << "x" << (tick % 1000000000LL);
    cfg.runTag = tag.str();

    const std::string exact = envOr("KAFKA_SOAK_TOPIC", std::string());
    if (!exact.empty()) {
        cfg.topic = exact;
        cfg.explicitTopic = true;
    } else {
        cfg.topic = envOr("KAFKA_TEST_TOPIC", "onec-librdkafka-soak-test") + "-" + cfg.runTag;
    }

    cfg.batch = std::max<long long>(1, std::min(cfg.batch, cfg.messages));

    // Samples can only be taken at batch boundaries, so the batch is what caps
    // how many points the regression gets. A short run with a big batch would
    // otherwise sail through the loop and then fail the verdict for having four
    // samples - the most annoying possible outcome. Shrink the batch instead,
    // and say so.
    const long long targetSamples = 16;
    const long long fittingBatch =
        std::max<long long>(1, std::min(cfg.batch, cfg.messages / targetSamples));
    if (fittingBatch < cfg.batch) {
        cfg.batchClampedFrom = cfg.batch;
        cfg.batch = fittingBatch;
    }

    // ~50 samples on a long run, never fewer than targetSamples on a short one,
    // so the regression always has enough points and the table stays readable.
    if (cfg.sampleEvery <= 0) {
        cfg.sampleEvery = std::max<long long>(cfg.batch, cfg.messages / 50);
    }
    cfg.sampleEvery = std::max<long long>(cfg.sampleEvery, 1);

#if defined(ONEC_SOAK_SANITIZER)
    cfg.rssUntrustworthy = "this binary is built with -fsanitize=" ONEC_SOAK_SANITIZER
                           "; its redzones and free-quarantine dominate RSS";
#endif
    if (cfg.rssUntrustworthy.empty() && runningUnderValgrind()) {
        cfg.rssUntrustworthy = "running under valgrind, whose own shadow state dominates RSS";
    }
    if (cfg.rssUntrustworthy.empty() && envFlag("KAFKA_SOAK_IGNORE_RSS")) {
        cfg.rssUntrustworthy = "KAFKA_SOAK_IGNORE_RSS is set";
    }

    if (!cfg.hardTimeoutSet) {
        // 3 ms per message is roughly thirty times slower than a local broker
        // manages through this component; anything past it is a hang, not load.
        const long long computed = 60000 + cfg.messages * 3;
        cfg.hardTimeoutMs = static_cast<int>(std::min<long long>(computed, 4LL * 3600 * 1000));
    }

    return cfg;
}

Config g_cfg;

//---------------------------------------------------------------------------//
// The payload
//---------------------------------------------------------------------------//

// Deliberately pure ASCII, and deliberately free of characters JSON would have
// to escape.
//
// This is now a measurement decision, not a workaround. It used to be a
// workaround: ComponentBase::toUTF8String sized its output buffer at two bytes
// per UTF-16 code unit, so anything in U+0800..U+FFFF (three UTF-8 bytes)
// overflowed it, iconv returned E2BIG and the truncated buffer was used anyway.
// That bug is fixed - the buffer is sized at three bytes per unit and the
// conversion result is honoured - and tests/kafka_ssl_test.cpp covers 3- and
// 4-byte characters and C0 control bytes directly.
//
// The payload stays ASCII because this test measures memory. One byte in, one
// byte converted, one byte escaped: any RSS or pool drift it reports is the
// component's, not an artefact of a payload whose UTF-8 and UTF-16 sizes differ
// by a factor that changes with the content. If the transient 3-bytes-per-unit
// conversion buffer is ever what you want to soak, that is a separate run with
// a separate baseline, not a change to this one.
std::string buildPayload(long long bytes)
{
    static const char alphabet[] =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.";
    const std::size_t span = sizeof(alphabet) - 1;

    std::string payload;
    payload.resize(static_cast<std::size_t>(bytes));
    for (std::size_t i = 0; i < payload.size(); ++i) {
        payload[i] = alphabet[i % span];
    }
    return payload;
}

// [{"Key":"<tag>-000000","Value":"<payload>","Headers":[{"n":"000000"}]},...]
//
// The format SetJSONMessageList expects (README.md, "API отправителя"): each
// Headers element is an object keyed by the header name, which is NOT the
// {"Key":..,"Value":..} shape the consumer hands back.
//
// The buffer is reused across batches on purpose: the test process must be flat
// in memory too, otherwise it measures itself.
void buildBatchJson(std::string& out, const std::string& runTag, const std::string& payload,
                    long long firstIndex, long long count)
{
    out.clear();
    out.push_back('[');
    for (long long i = 0; i < count; ++i) {
        if (i != 0) {
            out.push_back(',');
        }
        std::ostringstream index;
        index << std::setw(9) << std::setfill('0') << (firstIndex + i);
        const std::string n = index.str();

        out += "{\"Key\":\"";
        out += runTag;
        out += '-';
        out += n;
        out += "\",\"Value\":\"";
        out += payload;
        out += "\",\"Headers\":[{\"n\":\"";
        out += n;
        out += "\"}]}";
    }
    out.push_back(']');
}

//---------------------------------------------------------------------------//
// Samples
//---------------------------------------------------------------------------//

struct Sample
{
    long long messages   = 0;
    long long rss        = 0;
    long      poolLength = 0;
    long long liveBlocks = 0;
    long      fds        = 0;
    double    seconds    = 0.0;
};

struct Fit
{
    double slope = 0.0;        // bytes of RSS per message
    double r     = 0.0;        // Pearson correlation, for diagnosis only
    bool   valid = false;
};

Fit fitSlope(const std::vector<Sample>& samples)
{
    Fit fit;
    if (samples.size() < 2) {
        return fit;
    }

    double sumX = 0.0;
    double sumY = 0.0;
    for (const Sample& s : samples) {
        sumX += static_cast<double>(s.messages);
        sumY += static_cast<double>(s.rss);
    }
    const double n = static_cast<double>(samples.size());
    const double meanX = sumX / n;
    const double meanY = sumY / n;

    double sxx = 0.0;
    double syy = 0.0;
    double sxy = 0.0;
    for (const Sample& s : samples) {
        const double dx = static_cast<double>(s.messages) - meanX;
        const double dy = static_cast<double>(s.rss) - meanY;
        sxx += dx * dx;
        syy += dy * dy;
        sxy += dx * dy;
    }
    if (sxx <= 0.0) {
        return fit;
    }

    fit.slope = sxy / sxx;
    fit.r = (syy > 0.0) ? sxy / std::sqrt(sxx * syy) : 0.0;
    fit.valid = true;
    return fit;
}

std::vector<Sample> g_samples;
long long           g_produced = 0;
long long           g_consumed = 0;
long long           g_maxPool  = 0;
bool                g_ranOut   = false;   // stopped by --max-seconds

// --self-test only: a deliberate, reachable, per-message leak of exactly the
// shape the consumer's local message pool would have. It is the calibration
// weight for the detector - see retainBallast().
std::vector<char> g_ballast;

// Grows the ballast by selfTestBytes per message and touches every new page, so
// the bytes are resident rather than merely reserved. Nothing in a normal run
// ever calls this with a non-zero size.
void retainBallast(long long messages)
{
    if (g_cfg.selfTestBytes <= 0 || messages <= 0) {
        return;
    }
    const std::size_t grow = static_cast<std::size_t>(messages * g_cfg.selfTestBytes);
    const std::size_t was = g_ballast.size();
    g_ballast.resize(was + grow);
    // resize() value-initialises, which already writes every byte; the explicit
    // loop below only guards against that being optimised away for a POD vector.
    for (std::size_t i = was; i < g_ballast.size(); i += 4096) {
        g_ballast[i] = static_cast<char>(i & 0x7F);
    }
}

void printHeader()
{
    std::cout << "\n"
              << "      messages          RSS        dRSS   pool  blocks   fds     msg/s\n"
              << "  -------------------------------------------------------------------" << std::endl;
}

void printSample(const Sample& s, const Sample* previous)
{
    const long long delta = (previous != nullptr) ? s.rss - previous->rss : 0;
    const double    rate  = (s.seconds > 0.0) ? static_cast<double>(s.messages) / s.seconds : 0.0;

    std::ostringstream row;
    row << "  " << std::setw(12) << s.messages
        << "  " << std::setw(11) << humanBytes(s.rss)
        << "  " << std::setw(10) << (previous != nullptr ? signedBytes(delta) : std::string("-"))
        << "  " << std::setw(5) << s.poolLength
        << "  " << std::setw(6) << s.liveBlocks
        << "  " << std::setw(4) << s.fds
        << "  " << std::setw(8) << std::fixed << std::setprecision(0) << rate;
    std::cout << row.str() << std::endl;
}

Sample takeSample(long long messages, ComponentObject& producer, ComponentObject& consumer)
{
    Sample s;
    s.messages = messages;
    s.rss = rssBytes();
    s.poolLength = consumer.callLong(u"GetMessagePoolLength");
    s.liveBlocks = static_cast<long long>(producer.memory().liveBlocks() +
                                          consumer.memory().liveBlocks());
    s.fds = openFdCount();
    s.seconds = elapsedSeconds();
    return s;
}

//---------------------------------------------------------------------------//
// Shared SSL wiring - identical to kafka_ssl_test::applySslConf
//---------------------------------------------------------------------------//

void applySslConf(ComponentObject& object, const std::string& clientId)
{
    REQUIRE(object.callBool(u"SetGlobalConf", {"security.protocol", "SSL"}));
    REQUIRE(object.callBool(u"SetGlobalConf", {"ssl.ca.location", g_cfg.caPath}));
    REQUIRE(object.callBool(u"SetGlobalConf", {"enable.ssl.certificate.verification", "true"}));
    REQUIRE(object.callBool(u"SetGlobalConf", {"client.id", clientId}));
}

//---------------------------------------------------------------------------//
// One produce -> consume -> drain cycle
//---------------------------------------------------------------------------//

// Produces `count` messages starting at `firstIndex` and returns false on the
// first hard failure, so the caller can stop the run instead of grinding
// through another 999 batches of the same error.
bool produceBatch(ComponentObject& producer, std::string& json, const std::string& payload,
                  long long firstIndex, long long count)
{
    buildBatchJson(json, g_cfg.runTag, payload, firstIndex, count);

    if (!producer.callBool(u"SetJSONMessageList", {json})) {
        FAIL("SetJSONMessageList rejected batch at message " + std::to_string(firstIndex) + ": " +
             producer.errorDescription() + "; json=" + clip(json));
        return false;
    }
    if (!producer.callBool(u"Produce")) {
        FAIL("Produce() failed at message " + std::to_string(firstIndex) + ": " +
             producer.errorDescription());
        return false;
    }
    // Produce() returns true even when every message timed out; IsDelivered() is
    // where the truth lives.
    if (!producer.callBool(u"IsDelivered")) {
        FAIL("batch at message " + std::to_string(firstIndex) + " was not delivered: " +
             clip(producer.callString(u"GetJSONDeliveryReport"), 1200));
        return false;
    }
    // Produce() clears the pool on success - if it ever stopped doing so, the
    // producer side would grow without bound too.
    const long left = producer.callLong(u"GetMessagePoolLength");
    if (left != 0) {
        FAIL("the producer pool still holds " + std::to_string(left) +
             " message(s) after a successful Produce() at message " + std::to_string(firstIndex));
        return false;
    }
    return true;
}

// Polls until `count` messages of this run are in the pool or the deadline
// passes, then drains the pool through ReceiveJSONMessages + ClearMessagePool.
// Returns the number of messages drained, or -1 on a hard failure.
long long consumeBatch(ComponentObject& consumer, long long count, int timeoutMs,
                       bool verifyContents, const std::string& payload)
{
    const Clock::time_point deadline = Clock::now() + std::chrono::milliseconds(timeoutMs);

    long poolLength = consumer.callLong(u"GetMessagePoolLength");
    while (poolLength < count && Clock::now() < deadline) {
        // errors_count_to_interrupt must be >= 1: ConsumePool rejects 0 outright
        // ("Bad parametrs: invalid interrupt errors count"). With 1 the batch
        // stops at the first empty poll, so one call costs at most pollMs.
        const long rc = consumer.callLong(
            u"ConsumePool", {g_cfg.pollMs, static_cast<int>(count - poolLength), 1});
        if (rc == kConsumerFatalError) {
            FAIL("ConsumePool reported a fatal error (FatalError=" +
                 std::string(consumer.fatalError() ? "true" : "false") +
                 "): " + consumer.errorDescription());
            return -1;
        }
        if (rc != kConsumerError && rc != kConsumerNoError) {
            FAIL("ConsumePool returned an unexpected value: " + std::to_string(rc));
            return -1;
        }
        poolLength = consumer.callLong(u"GetMessagePoolLength");
    }

    if (poolLength <= 0) {
        FAIL("no messages came back within " + std::to_string(timeoutMs) + " ms (expected " +
             std::to_string(count) + "); last consumer status: " + consumer.errorDescription());
        return -1;
    }
    g_maxPool = std::max<long long>(g_maxPool, poolLength);

    // The JSON build is the memory-heavy path in the component (it walks the
    // whole pool and allocates one string through the memory manager), so it is
    // exercised on every batch rather than occasionally.
    const std::string json = consumer.callString(u"ReceiveJSONMessages", {false});
    if (json.empty()) {
        FAIL("ReceiveJSONMessages returned nothing with " + std::to_string(poolLength) +
             " message(s) in the pool: " + consumer.errorDescription());
        return -1;
    }

    // Cheap, exact, and it does not allocate per message. The marker has to be
    // the whole "Key":"<tag>- prefix rather than the bare run tag: the tag is
    // also in the per-run topic name, which the component repeats in every
    // record, so counting the tag alone would count every message twice.
    // A mismatch means the component dropped or duplicated messages between the
    // pool and the JSON it builds from it.
    const std::size_t seen = countOccurrences(json, "\"Key\":\"" + g_cfg.runTag + "-");
    if (seen != static_cast<std::size_t>(poolLength)) {
        FAIL("ReceiveJSONMessages emitted " + std::to_string(seen) + " message(s) of run " +
             g_cfg.runTag + " but the pool holds " + std::to_string(poolLength));
        return -1;
    }

    // A full content check is worth one batch, not a million: it confirms the
    // payload survives the UTF-16 round trip, which is a correctness question
    // the TLS test already owns.
    if (verifyContents) {
        CHECK_MSG(json.find("\"Value\":\"" + payload + "\"") != std::string::npos,
                  "the payload did not survive the round trip; first 400 bytes: " + clip(json));
    }

    if (!consumer.callBool(u"ClearMessagePool")) {
        FAIL("ClearMessagePool failed: " + consumer.errorDescription());
        return -1;
    }
    // The point of the whole exercise: the pool must be empty again. If this
    // ever fails the growth is not subtle and the run should stop here.
    const long afterClear = consumer.callLong(u"GetMessagePoolLength");
    if (afterClear != 0) {
        FAIL("GetMessagePoolLength is " + std::to_string(afterClear) +
             " after ClearMessagePool - the component's local message pool does not drain");
        return -1;
    }

    return poolLength;
}

//---------------------------------------------------------------------------//
// The soak
//---------------------------------------------------------------------------//

void caseSoak(ComponentLibrary& lib)
{
    const std::string payload = buildPayload(g_cfg.payloadBytes);

    phase("creating KafkaProducer / KafkaConsumer");
    ComponentObject producer(lib, u"KafkaProducer");
    ComponentObject consumer(lib, u"KafkaConsumer");

    phase("producer: SSL configuration (ca=" + g_cfg.caPath + ")");
    applySslConf(producer, g_cfg.client("producer"));
    // message.timeout.ms has to go through SetTopicConf: Initialize calls
    // conf->set("default_topic_conf", tconf), which would drop a global value.
    // It is also the only thing bounding Produce(), which blocks in
    //   while (producer->outq_len() > 0) producer->poll(1000);
    // with no timeout of its own (src/producer1c_core.cpp).
    REQUIRE(producer.callBool(u"SetTopicConf",
                              {"message.timeout.ms", std::to_string(g_cfg.produceTimeoutMs)}));
    REQUIRE_MSG(producer.callBool(u"Initialize", {g_cfg.bootstrap, g_cfg.topic, -1}),
                "producer Initialize failed: " + producer.errorDescription());

    phase("consumer: SSL configuration");
    applySslConf(consumer, g_cfg.client("consumer"));
    REQUIRE(consumer.callBool(u"SetGlobalConf", {"enable.auto.commit", "false"}));
    // auto.offset.reset is a topic-level property; set globally it would be
    // thrown away when Initialize installs the component's own topic conf.
    REQUIRE(consumer.callBool(u"SetTopicConf", {"auto.offset.reset", "earliest"}));
    REQUIRE_MSG(consumer.callBool(u"Initialize", {g_cfg.bootstrap, g_cfg.group()}),
                "consumer Initialize failed: " + consumer.errorDescription());

    // The drain loop below clears the pool explicitly and asserts it is empty.
    // That only means anything while the component is not draining it silently
    // on every JSON build, so assert the default rather than assume it.
    CHECK_FALSE(consumer.getProp(u"RemoveMessagesFromLocalQueueOnJSONBuild").asBool());

    phase("consumer: subscribing to '" + g_cfg.topic + "'");
    REQUIRE(consumer.callBool(u"AddTopicToSubscribeList", {g_cfg.topic}));
    REQUIRE_MSG(consumer.callBool(u"Subscribe"), "Subscribe failed: " + consumer.errorDescription());

    //-----------------------------------------------------------------------//
    // Warm-up batch: creates the topic, joins the group, fills librdkafka's
    // buffers. Its timings and its memory are both meaningless, which is why it
    // happens before the first sample.
    //-----------------------------------------------------------------------//

    const long long warmupCount = std::min<long long>(g_cfg.batch, g_cfg.messages);
    phase("warm-up: " + std::to_string(warmupCount) + " message(s), topic auto-create + group join");

    std::string json;
    json.reserve(static_cast<std::size_t>(g_cfg.batch * (g_cfg.payloadBytes + 80)));

    REQUIRE_MSG(produceBatch(producer, json, payload, 0, warmupCount),
                "the warm-up batch could not be produced - see the failure above");
    const long long warmed = consumeBatch(consumer, warmupCount, g_cfg.warmupTimeoutMs, true, payload);
    REQUIRE_MSG(warmed > 0, "the warm-up batch never came back - see the failure above");
    g_produced += warmupCount;
    g_consumed += warmed;

    //-----------------------------------------------------------------------//
    // The loop
    //-----------------------------------------------------------------------//

    const Clock::time_point budget =
        Clock::now() + std::chrono::seconds(g_cfg.maxSeconds > 0 ? g_cfg.maxSeconds : 0);

    phase("soak: " + std::to_string(g_cfg.messages) + " message(s), batch " +
          std::to_string(g_cfg.batch) + ", sample every " + std::to_string(g_cfg.sampleEvery));
    printHeader();

    g_samples.push_back(takeSample(g_produced, producer, consumer));
    printSample(g_samples.back(), nullptr);

    long long nextSampleAt = g_produced + g_cfg.sampleEvery;
    bool      broken = false;

    while (g_produced < g_cfg.messages) {
        if (g_cfg.maxSeconds > 0 && Clock::now() >= budget) {
            g_ranOut = true;
            break;
        }

        const long long count = std::min<long long>(g_cfg.batch, g_cfg.messages - g_produced);
        if (!produceBatch(producer, json, payload, g_produced, count)) {
            broken = true;
            break;
        }
        g_produced += count;
        retainBallast(count);   // no-op unless --self-test says otherwise

        const long long got = consumeBatch(consumer, count, g_cfg.batchTimeoutMs, false, payload);
        if (got < 0) {
            broken = true;
            break;
        }
        g_consumed += got;

        if (g_produced >= nextSampleAt || g_produced >= g_cfg.messages) {
            const Sample previous = g_samples.back();
            g_samples.push_back(takeSample(g_produced, producer, consumer));
            printSample(g_samples.back(), &previous);
            nextSampleAt = g_produced + g_cfg.sampleEvery;

            // liveBlocks() must be zero between calls: the harness Value objects
            // release everything the component allocated as they go, so a
            // non-zero reading here is the component holding a block the host
            // was supposed to free - or the harness doing so, which is just as
            // much of a bug.
            CHECK_MSG(g_samples.back().liveBlocks == 0,
                      "the memory manager holds " + std::to_string(g_samples.back().liveBlocks) +
                          " block(s) after " + std::to_string(g_produced) +
                          " message(s); it should be empty between calls");
        }
    }

    std::cout << std::endl;

    if (g_ranOut) {
        test::note("the " + std::to_string(g_cfg.maxSeconds) +
                   " s budget ran out after " + std::to_string(g_produced) + " of " +
                   std::to_string(g_cfg.messages) +
                   " message(s); the analysis below covers what was actually done");
    }
    REQUIRE_MSG(!broken, "the soak loop stopped early - see the failure above");

    //-----------------------------------------------------------------------//
    // Final state
    //-----------------------------------------------------------------------//

    phase("wind-down: Commit / Unsubscribe / ClearMessagePool");
    CHECK_MSG(consumer.callBool(u"Commit"), "Commit failed: " + consumer.errorDescription());
    CHECK_MSG(consumer.callBool(u"Unsubscribe"), "Unsubscribe failed: " + consumer.errorDescription());
    CHECK_MSG(consumer.callBool(u"ClearMessagePool"),
              "final ClearMessagePool failed: " + consumer.errorDescription());
    CHECK_EQ(consumer.callLong(u"GetMessagePoolLength"), 0L);
    CHECK_FALSE(consumer.fatalError());

    CHECK_MSG(consumer.memory().liveBlocks() == 0,
              "the consumer left " + std::to_string(consumer.memory().liveBlocks()) +
                  " memory manager block(s) allocated at the end of the run");
    CHECK_MSG(producer.memory().liveBlocks() == 0,
              "the producer left " + std::to_string(producer.memory().liveBlocks()) +
                  " memory manager block(s) allocated at the end of the run");

    test::note("memory manager totals: producer " +
               std::to_string(producer.memory().totalBlocks()) + " block(s), consumer " +
               std::to_string(consumer.memory().totalBlocks()) + " block(s), all released");

    // A soak that quietly consumed nothing would sail through every memory
    // check above.
    if (g_cfg.explicitTopic) {
        CHECK_MSG(g_consumed >= g_produced,
                  "only " + std::to_string(g_consumed) + " of " + std::to_string(g_produced) +
                      " produced message(s) came back");
    } else {
        CHECK_MSG(g_consumed == g_produced,
                  std::to_string(g_consumed) + " message(s) came back out of " +
                      std::to_string(g_produced) + " produced on a topic nobody else writes to");
    }
}

//---------------------------------------------------------------------------//
// The verdict
//---------------------------------------------------------------------------//

void caseMemoryVerdict()
{
    REQUIRE_MSG(g_samples.size() >= 2, "the soak loop produced no samples to analyse");

    // Discard the warm-up window: librdkafka's buffers, the glibc arena and the
    // component's own JSON pool all grow legitimately at the start of a run.
    // Never discard so much that the fit is left with too few points - a run cut
    // short by --max-seconds must still produce a verdict on what it did do.
    const std::size_t keepAtLeast = std::min(g_samples.size(), kMinSteadySamples);
    const std::size_t wanted = static_cast<std::size_t>(
        static_cast<long long>(g_samples.size()) * g_cfg.warmupPercent / 100);
    const std::size_t discard = std::min(wanted, g_samples.size() - keepAtLeast);
    const std::vector<Sample> steady(g_samples.begin() + static_cast<std::ptrdiff_t>(discard),
                                     g_samples.end());

    const Sample&   first = steady.front();
    const Sample&   last = steady.back();
    const long long steadyMessages = last.messages - first.messages;
    const long long steadyGrowth = last.rss - first.rss;

    test::note("samples: " + std::to_string(g_samples.size()) + " total, " +
               std::to_string(discard) + " discarded as warm-up, " + std::to_string(steady.size()) +
               " in steady state (messages " + std::to_string(first.messages) + " -> " +
               std::to_string(last.messages) + ")");
    test::note("RSS: " + humanBytes(g_samples.front().rss) + " at start, " +
               humanBytes(first.rss) + " after warm-up, " + humanBytes(last.rss) + " at end (" +
               signedBytes(steadyGrowth) + " across the steady window)");
    test::note("peak pool length " + std::to_string(g_maxPool) + " message(s), " +
               std::to_string(g_consumed) + " consumed in " +
               std::to_string(static_cast<long long>(elapsedSeconds())) + " s");

    REQUIRE_MSG(steady.size() >= kMinSteadySamples,
                "only " + std::to_string(steady.size()) +
                    " steady-state sample(s); a slope fitted through fewer than " +
                    std::to_string(kMinSteadySamples) +
                    " points is noise. Lower KAFKA_SOAK_SAMPLE_EVERY or raise KAFKA_SOAK_MESSAGES");
    REQUIRE_MSG(steadyMessages > 0, "the steady-state window covers no messages");

    const Fit fit = fitSlope(steady);
    REQUIRE_MSG(fit.valid, "the RSS samples could not be fitted (all at the same message count?)");

    // Two rules, either of which fails the run. The full argument for both is in
    // the header comment; the short version is that rule A catches a big leak on
    // any window without ever firing on an allocator staircase, and rule B
    // catches a small leak that rule A's amortised allowance would swallow on a
    // short window - because a leak draws a straight line and a staircase does
    // not.
    const double floorBytes = static_cast<double>(g_cfg.maxBytesPerMsg);
    const double steadyMessagesF = static_cast<double>(steadyMessages);

    // A. magnitude
    const double noiseTerm = static_cast<double>(g_cfg.rssNoiseBytes) / steadyMessagesF;
    const double magnitudeBar = floorBytes + noiseTerm;
    const bool   magnitudeFired = fit.slope > magnitudeBar;

    // B. linearity
    const double linearBar =
        std::max(floorBytes, static_cast<double>(g_cfg.minLinearBytes) / steadyMessagesF);
    const bool linearFired = fit.r >= kLinearR && fit.slope > linearBar &&
                             steadyGrowth > g_cfg.minLinearBytes;

    // What this run was actually able to rule out. Printed on success too: a
    // green soak over 5000 messages proves much less than a green soak over a
    // million, and the reader should not have to work that out.
    //
    // Two floors, not one. Rule A is unconditional: any leak above magnitudeBar
    // is caught whatever the data looks like. Rule B is NOT - it only fires when
    // the growth is clean enough to reach r >= kLinearR, so linearBar is a floor
    // this run reaches only if the window is long enough for the allocator's
    // start-up transient to fall outside the steady region.
    //
    // Reporting min(A, B) as "the" floor overstated the run's sensitivity, and
    // measurably so. Measured on this box at --messages=10000 (12 steady
    // samples, window 3125..10000, rule A bar 2472 B/msg, rule B bar 305 B/msg):
    //   --self-test=2600  -> caught (rule A)
    //   --self-test=512   -> MISSED, r = 0.967..0.970 over three runs
    //   --self-test=400   -> MISSED, r = 0.950
    // The old line claimed a 305 B/msg floor on that window while the true
    // floor was rule A's 2472 - an 8x overstatement. The cause is that a 25%
    // warm-up discard is a relative window but the allocator transient is absolute
    // one: at 10000 messages the +1.5 MiB arena step at message 3750 lands
    // INSIDE the steady region and holds r under the bar. At --quick (20000
    // messages, 30 steady samples) the same transient is discarded and the very
    // same 512 B/msg leak is caught at r = 1.000.
    //
    // Only the reporting is fixed here - the two rules are untouched. Making
    // rule B reachable on short windows is a threshold change and belongs to
    // whoever owns the soak design, not to this integration pass.
    const double unconditionalFloor = magnitudeBar;
    const double bestCaseFloor      = std::min(magnitudeBar, linearBar);
    const bool   linearReachable    = fit.r >= kLinearR;

    // Used only to point at the first suspicious sample, so the optimistic
    // number is the useful one there.
    const double detectionFloor = bestCaseFloor;

    std::ostringstream verdict;
    verdict << std::fixed << std::setprecision(2) << "RSS slope " << fit.slope
            << " B/message (r=" << std::setprecision(3) << fit.r << "); rule A bar "
            << std::setprecision(2) << magnitudeBar << " B/message (" << g_cfg.maxBytesPerMsg
            << " floor + " << noiseTerm << " noise amortised over " << steadyMessages
            << " message(s)), rule B bar " << linearBar << " B/message at r>=" << kLinearR
            << " with at least " << humanBytes(g_cfg.minLinearBytes) << " of growth";
    test::note(verdict.str());

    std::ostringstream projected;
    projected << std::fixed << std::setprecision(1)
              << "projected at this slope: " << (fit.slope * 1e6 / (1024.0 * 1024.0))
              << " MiB per million messages";
    test::note(projected.str());

    // Say what was really ruled out, and do not round it in the run's favour.
    std::ostringstream floors;
    floors << std::fixed << std::setprecision(0) << "this run detects a leak from "
           << unconditionalFloor << " B/message upwards unconditionally (rule A)";
    if (bestCaseFloor < unconditionalFloor) {
        floors << "; rule B lowers that to " << bestCaseFloor
               << " B/message ONLY for growth linear enough to reach r>=" << std::setprecision(2)
               << kLinearR << ", which this window " << (linearReachable ? "did" : "did NOT")
               << " achieve (r=" << std::setprecision(3) << fit.r << ")";
    }
    test::note(floors.str());

    if (bestCaseFloor < unconditionalFloor && !linearReachable) {
        // Two very different reasons for r to miss the bar, and they must not be
        // reported as the same thing. A flat, healthy run has a low or negative
        // r simply because there is no growth to correlate - that is the good
        // case. A run that IS growing above the optimistic floor but still fits
        // badly is the dangerous one: it means a real per-message leak of that
        // size would slip past rule B on a window this short.
        const bool growingButUnfitted = fit.slope > bestCaseFloor;
        std::string why =
            growingButUnfitted
                ? "RSS is growing faster than the rule B bar but the fit is too poor to "
                  "trigger it - on a window this short the allocator's start-up transient "
                  "stays inside the steady region and holds r down, so a real leak of this "
                  "size would go unreported. "
                : "the growth is not linear enough for rule B, which on a run with no "
                  "measurable growth is simply what healthy looks like. ";
        test::note("  so treat " + std::to_string(static_cast<long long>(unconditionalFloor)) +
                   " B/message as this run's real sensitivity, not " +
                   std::to_string(static_cast<long long>(bestCaseFloor)) + ": " + why +
                   "Raise KAFKA_SOAK_MESSAGES for a stronger claim.");
    }

    const bool exceeded = magnitudeFired || linearFired;

    // Say WHERE it started growing: the first steady sample whose own per-message
    // delta already exceeded the detection floor is the one to look at.
    std::string firstBad = "(no single sample exceeds the floor on its own - "
                           "the growth is spread evenly across the window)";
    for (std::size_t i = 1; i < steady.size(); ++i) {
        const long long dMessages = steady[i].messages - steady[i - 1].messages;
        const long long dRss = steady[i].rss - steady[i - 1].rss;
        if (dMessages > 0 &&
            static_cast<double>(dRss) / static_cast<double>(dMessages) > detectionFloor) {
            firstBad = "first sample over budget: message " +
                       std::to_string(steady[i - 1].messages) + " -> " +
                       std::to_string(steady[i].messages) + ", RSS " +
                       humanBytes(steady[i - 1].rss) + " -> " + humanBytes(steady[i].rss) + " (" +
                       signedBytes(dRss) + ")";
            break;
        }
    }

    if (!g_cfg.rssUntrustworthy.empty()) {
        // Not a pass and not a failure: the number this rule reads is not the
        // component's. Say so in the loudest terms the harness has, so nobody
        // reads a green sanitizer soak as "no unbounded growth".
        test::note("RSS GROWTH RULE NOT APPLIED: " + g_cfg.rssUntrustworthy + ".");
        test::note("  The slope above is this build's allocator, not the component. Unbounded "
                   "growth was NOT ruled out by this run - do that with a plain build. What this "
                   "run does prove is below: the pool drains, liveBlocks() stays at zero, the fd "
                   "count is flat, and the sanitizer's own report stands on its own.");
        if (g_cfg.selfTestBytes > 0) {
            test::note("  --self-test cannot prove anything here either, for the same reason.");
        }
    } else if (g_cfg.selfTestBytes > 0) {
        // Inverted: the run deliberately retained selfTestBytes per message, so
        // a detector that stays quiet is the bug.
        // A self-test below this run's unconditional floor is asking the run to
        // catch something it never claimed to catch, so say which of the two
        // things went wrong rather than always blaming the threshold.
        const bool belowUnconditional =
            static_cast<double>(g_cfg.selfTestBytes) < unconditionalFloor;
        const std::string diagnosis =
            belowUnconditional
                ? ". Rule A never claimed this leak: " + std::to_string(g_cfg.selfTestBytes) +
                      " B/message is below this run's unconditional floor of " +
                      std::to_string(static_cast<long long>(unconditionalFloor)) +
                      " B/message, and rule B could not take over because the fit reached only "
                      "r=" +
                      std::to_string(fit.r) +
                      " - a window this short keeps the allocator's start-up transient inside the "
                      "steady region. Raise KAFKA_SOAK_MESSAGES (--quick catches 512 B/message at "
                      "r=1.000), or self-test above the unconditional floor"
                : ". The growth threshold is not doing its job - a real pool leak would pass "
                  "unnoticed too";
        CHECK_MSG(exceeded,
                  "--self-test=" + std::to_string(g_cfg.selfTestBytes) +
                      " retained " + std::to_string(g_cfg.selfTestBytes) +
                      " B/message on purpose and the RSS detector did not fire. " + verdict.str() +
                      diagnosis);
        if (exceeded) {
            test::note(std::string("self-test: the detector fired as it should (rule ") +
                       (magnitudeFired ? "A" : "") + (linearFired ? "B" : "") + "). " + firstBad);
        }
    } else if (exceeded) {
        FAIL(std::string("rule ") + (magnitudeFired ? "A (magnitude)" : "") +
             (magnitudeFired && linearFired ? " and " : "") +
             (linearFired ? "B (linearity)" : "") + " fired: " + verdict.str() +
             ". The resident set grows with the message count, which is the "
                             "failure this test exists for: the memory may still be reachable, so "
                             "ASan will not say a word, but the 1C process will die on a long "
                             "queue. " +
             firstBad);
    } else {
        test::reportPass();
    }

    //-----------------------------------------------------------------------//
    // File descriptors
    //-----------------------------------------------------------------------//

    const long fdGrowth = last.fds - first.fds;
    test::note("file descriptors: " + std::to_string(g_samples.front().fds) + " at start, " +
               std::to_string(first.fds) + " after warm-up, " + std::to_string(last.fds) +
               " at end");
    CHECK_MSG(fdGrowth <= g_cfg.maxFdGrowth,
              "the process opened " + std::to_string(fdGrowth) +
                  " more file descriptor(s) during the steady-state window (" +
                  std::to_string(first.fds) + " -> " + std::to_string(last.fds) +
                  "), more than the " + std::to_string(g_cfg.maxFdGrowth) +
                  " allowed. An fd leak at production message rates hits the rlimit and the "
                  "component stops being able to talk to the broker at all");

    //-----------------------------------------------------------------------//
    // Pool and memory manager
    //-----------------------------------------------------------------------//

    for (const Sample& s : g_samples) {
        if (s.poolLength != 0) {
            FAIL("the component's message pool held " + std::to_string(s.poolLength) +
                 " message(s) at the sample taken after " + std::to_string(s.messages) +
                 " message(s) - every batch is drained with ClearMessagePool, so it must read 0");
            return;
        }
    }
    test::reportPass();
}

//---------------------------------------------------------------------------//
// Preflight
//---------------------------------------------------------------------------//

void printBanner()
{
    std::cout << "=============================================================" << std::endl;
    std::cout << "onec-librdkafka soak / leak test" << std::endl;
    std::cout << "  component  : " << g_cfg.soPath << std::endl;
    std::cout << "  bootstrap  : " << g_cfg.bootstrap << std::endl;
    std::cout << "  ca         : " << g_cfg.caPath << std::endl;
    std::cout << "  topic      : " << g_cfg.topic
              << (g_cfg.explicitTopic ? "  (KAFKA_SOAK_TOPIC, shared)" : "  (per-run)") << std::endl;
    std::cout << "  messages   : " << g_cfg.messages << " x " << g_cfg.payloadBytes
              << " B payload, batch " << g_cfg.batch;
    if (g_cfg.batchClampedFrom > 0) {
        std::cout << "  (reduced from " << g_cfg.batchClampedFrom
                  << " so the run yields enough samples to fit a slope)";
    }
    std::cout << std::endl;
    std::cout << "  sampling   : every " << g_cfg.sampleEvery << " message(s), first "
              << g_cfg.warmupPercent << "% discarded as warm-up" << std::endl;
    std::cout << "  budget     : " << g_cfg.maxBytesPerMsg << " B/message + "
              << humanBytes(g_cfg.rssNoiseBytes) << " amortised noise allowance, "
              << g_cfg.maxFdGrowth << " fd(s)" << std::endl;
    std::cout << "  timeouts   : poll " << g_cfg.pollMs << " ms, produce " << g_cfg.produceTimeoutMs
              << " ms, batch " << g_cfg.batchTimeoutMs << " ms, hard " << g_cfg.hardTimeoutMs
              << " ms" << (g_cfg.hardTimeoutSet ? "" : " (derived)") << std::endl;
    if (g_cfg.maxSeconds > 0) {
        std::cout << "  wall clock : stop after " << g_cfg.maxSeconds << " s and analyse"
                  << std::endl;
    }
    if (g_cfg.selfTestBytes > 0) {
        std::cout << "  SELF-TEST  : retaining " << g_cfg.selfTestBytes
                  << " B/message on purpose; the run passes only if the detector FIRES"
                  << std::endl;
    }
    if (!g_cfg.rssUntrustworthy.empty()) {
        std::cout << "  RSS RULE   : DISABLED - " << g_cfg.rssUntrustworthy << std::endl;
        std::cout << "               the pool, liveBlocks(), fd and round-trip checks still apply"
                  << std::endl;
    }
    std::cout << "  run tag    : " << g_cfg.runTag << std::endl;
    std::cout << "=============================================================" << std::endl;
}

// Returns kExitOk when the run may proceed, otherwise the exit code to use.
int preflight()
{
    // A sampling interval that cannot yield enough points is a configuration
    // error, and saying so now beats saying so after a million messages.
    const long long expectedSamples =
        1 + (g_cfg.messages + g_cfg.sampleEvery - 1) / g_cfg.sampleEvery;
    if (expectedSamples < static_cast<long long>(kMinSteadySamples) + 1) {
        std::cerr << "CONFIGURATION ERROR: " << g_cfg.messages << " message(s) sampled every "
                  << g_cfg.sampleEvery << " give only " << expectedSamples
                  << " sample(s); at least " << (kMinSteadySamples + 1)
                  << " are needed to fit a slope anyone should believe." << std::endl;
        std::cerr << "                     Raise KAFKA_SOAK_MESSAGES / --messages, or lower "
                     "KAFKA_SOAK_SAMPLE_EVERY / --sample-every."
                  << std::endl;
        return kExitSetup;
    }

    const char* const verdict = g_cfg.requireBroker ? "FAIL" : "SKIP";
    const int         code = g_cfg.requireBroker ? kExitFailed : kExitSkipped;

    if (!fileExists(g_cfg.caPath)) {
        std::cout << verdict << ": the CA certificate '" << g_cfg.caPath << "' does not exist."
                  << std::endl;
        std::cout << "      Run tests/docker/up.sh first, or point KAFKA_SSL_CA at the broker CA."
                  << std::endl;
        if (g_cfg.requireBroker) {
            std::cout << "      KAFKA_TEST_REQUIRE_BROKER=1, so this is a failure, not a skip."
                      << std::endl;
        }
        return code;
    }

    std::string why;
    if (!tcpReachable(g_cfg.bootstrap, g_cfg.probeTimeoutMs, &why)) {
        std::cout << verdict << ": no Kafka broker listening on " << g_cfg.bootstrap << " (" << why
                  << ")." << std::endl;
        std::cout << "      Start it with tests/docker/up.sh, or set KAFKA_SSL_BOOTSTRAP."
                  << std::endl;
        if (g_cfg.requireBroker) {
            std::cout << "      KAFKA_TEST_REQUIRE_BROKER=1, so this is a failure, not a skip: the "
                         "run was supposed to provide a broker and did not."
                      << std::endl;
        } else {
            std::cout << "      Nothing was tested - this is a skip, not a pass." << std::endl;
        }
        return code;
    }
    return kExitOk;
}

} // namespace

//---------------------------------------------------------------------------//

int main(int argc, char** argv)
{
    bool proceed = true;
    int  exitCode = kExitOk;
    g_cfg = buildConfig(argc, argv, proceed, exitCode);
    if (!proceed) {
        return exitCode;
    }
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
        std::cerr << "               build it first (scripts/build-component-linux.sh), or pass "
                     "the .so path as argv[1] / $ONEC_KAFKA_SO."
                  << std::endl;
        return kExitSetup;
    }

    // librdkafka leaves background threads and OpenSSL state behind; unmapping
    // the library underneath them at exit buys nothing and can turn a green run
    // into a segfault after the last check.
    library->setCloseOnDestroy(false);
    ComponentLibrary& lib = *library;

    test::run("soak: " + std::to_string(g_cfg.messages) +
                  " messages through produce -> consume -> drain",
              [&lib] { caseSoak(lib); });
    test::run("memory: RSS, file descriptors and the message pool are flat in steady state",
              [] { caseMemoryVerdict(); });

    phase("done");
    return test::summary("soak") == 0 ? kExitOk : kExitFailed;
}
