// Smoke test for librdkafka_onec.so.
//
// Runs with no Kafka broker, no network and no certificates. Its job is to fail
// loudly when the build is broken rather than when the cluster is: a missing
// export, a class that no longer instantiates, a method whose arity drifted away
// from README.md, or - the reason this suite exists at all - a .so that got
// linked against the wrong librdkafka.
//
// The component was moved from librdkafka 2.3.0 to 2.15.1 because 2.3.0 cannot
// complete a TLS handshake against a Kafka 4.x broker. Everything the SSL tests
// prove rests on the right library actually being inside this .so, so that is
// asserted here, first, before any test goes near a broker.
//
// Build (from the repository root, one line):
//   g++ -std=c++17 -Wall -Wextra -I include -I tests tests/smoke_test.cpp
//       tests/host/onec_host.cpp tests/host/component_loader.cpp -ldl -pthread
//       -o smoke_test
//
// Run:
//   ./smoke_test [/path/to/librdkafka_onec.so]

#include <dlfcn.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "host/component_loader.h"
#include "host/onec_host.h"
#include "host/test_assert.h"

namespace {

//---------------------------------------------------------------------------//
// What this build is supposed to be
//---------------------------------------------------------------------------//

// Bump together with lib/. 2.3.0 is the version this component was rescued from;
// anything below 2.15.1 brings the Kafka 4.x TLS problem back.
const char* const kExpectedRdKafkaVersion = "2.15.1";

const char* const kExpectedClassNames[] = {
    "KafkaProducer",
    "KafkaConsumer",
    "KafkaAdminClient",
};

//---------------------------------------------------------------------------//
// Method and property tables
//---------------------------------------------------------------------------//

// params is what README.md documents for the method, which is also what
// AddFunctionProperty() passes as _countParam in src/*1c.cpp. GetNParams() has
// to agree with both: the 1C platform reads it to size the parameter array, and
// none of the method bodies look at lSizeArray, so a drift here is a memory bug
// in production, not a type error.
struct MethodSpec
{
    const char* name;
    long        params;
};

// src/producer1c.cpp:9-19; README.md "API отправителя".
const MethodSpec kProducerMethods[] = {
    { "Initialize",            3 },   // brokers, topic, partition
    { "Produce",               0 },
    { "ClearMessagePool",      0 },
    { "GetMessagePoolLength",  0 },
    { "SetGlobalConf",         2 },
    { "SetTopicConf",          2 },
    { "SetJSONMessageList",    1 },
    { "ConfReset",             0 },
    { "GetJSONDeliveryReport", 0 },
    { "IsDelivered",           0 },
};

// src/consumer1c.cpp:8-30; README.md "API получателя".
//
// The four entries marked "undocumented" are registered but absent from
// README.md. They are listed anyway - a registration this test does not name is
// a registration nothing guards. Note also that README.md spells
// ReceiveJSONMessages as "ReseiveJSONMessages"; the registered name, and the one
// 1C code has to use, is the one below.
const MethodSpec kConsumerMethods[] = {
    { "Initialize",                    2 },   // brokers, group id
    { "ClearMessagePool",              0 },
    { "GetMessagePoolLength",          0 },
    { "SetGlobalConf",                 2 },
    { "SetTopicConf",                  2 },
    { "ConfReset",                     0 },
    { "Subscribe",                     0 },
    { "AddTopicToSubscribeList",       1 },
    { "ClearSubscribeList",            0 },
    { "Unsubscribe",                   0 },
    { "Unassign",                      0 },
    { "Commit",                        0 },
    { "Assign",                        0 },
    { "AddRecordToTopicPartitionList", 3 },   // topic, partition, offset
    { "ClearTopicPartitionList",       0 },
    { "ReceiveJSONMessages",           1 },
    { "ReceiveOnecInternalMessages",   1 },   // undocumented
    { "QueryWatermarkOffsets",         3 },   // topic, partition, timeout
    { "CommittedOffset",               3 },   // topic, partition, timeout
    { "ConsumePool",                   3 },   // timeout, count, error budget
    { "SetLogFilePath",                1 },   // undocumented
    { "AppendHeaderFilter",            1 },   // undocumented
    { "ClearHeaderFilter",             0 },   // undocumented
};

// src/admin_client1c.cpp:8-19; README.md "API клиента администрирования".
const MethodSpec kAdminMethods[] = {
    { "Initialize",                    1 },   // brokers
    { "SetGlobalConf",                 2 },
    { "ConfReset",                     0 },
    { "AddRecordToTopicPartitionList", 3 },
    { "ClearTopicPartitionList",       0 },
    { "DeleteRecordsBefore",           1 },
    { "GetGroupOffsets",               2 },
    { "GetGroupList",                  1 },
    { "GetMetadata",                   2 },
    { "AlterGroupOffsets",             2 },
    { "DeleteGroupOffsets",            2 },
    { "QueryWatermarkOffsets",         1 },
};

const char* const kProducerProps[] = {
    "DecodeBase64Key",
    "DecodeBase64Value",
    "DecodeBase64HeadersValue",
    "CheckJSONFieldsForStringType",
    "ErrorDescription",
};

const char* const kConsumerProps[] = {
    "RemoveMessagesFromLocalQueueOnJSONBuild",
    "MaxMessagesInLocalQueue",
    "EscapeMessageValue",
    "EscapeMessageKey",
    "EscapeMessageHeaderValue",
    "EscapeMessageHeaderKey",
    "ErrorDescription",
    "FatalError",
};

const char* const kAdminProps[] = {
    "ErrorDescription",
};

struct ClassSpec
{
    const char*        className;
    const MethodSpec*  methods;
    std::size_t        methodCount;
    const char* const* props;
    std::size_t        propCount;
};

const ClassSpec kClasses[] = {
    { "KafkaProducer",    kProducerMethods, sizeof(kProducerMethods) / sizeof(kProducerMethods[0]),
                          kProducerProps,   sizeof(kProducerProps) / sizeof(kProducerProps[0]) },
    { "KafkaConsumer",    kConsumerMethods, sizeof(kConsumerMethods) / sizeof(kConsumerMethods[0]),
                          kConsumerProps,   sizeof(kConsumerProps) / sizeof(kConsumerProps[0]) },
    { "KafkaAdminClient", kAdminMethods,    sizeof(kAdminMethods) / sizeof(kAdminMethods[0]),
                          kAdminProps,      sizeof(kAdminProps) / sizeof(kAdminProps[0]) },
};

//---------------------------------------------------------------------------//
// Locating the .so
//---------------------------------------------------------------------------//

bool isRegularFile(const std::string& path)
{
    struct stat info;
    return ::stat(path.c_str(), &info) == 0 && S_ISREG(info.st_mode);
}

std::string executableDir()
{
    std::string buffer(4096, '\0');
    const ssize_t written = ::readlink("/proc/self/exe", &buffer[0], buffer.size() - 1);
    if (written <= 0) {
        return std::string();
    }
    buffer.resize(static_cast<std::size_t>(written));
    const std::size_t slash = buffer.find_last_of('/');
    return slash == std::string::npos ? std::string(".") : buffer.substr(0, slash);
}

// argv[1] wins, then $ONEC_KAFKA_SO, then the usual spots relative to the test
// binary itself (a build directory next to out64/, or the repository root), then
// whatever the harness would have picked relative to the working directory.
std::string resolveSoPath(int argc, char** argv)
{
    if (argc > 1 && argv[1] != nullptr && argv[1][0] != '\0') {
        return std::string(argv[1]);
    }
    if (const char* fromEnv = std::getenv("ONEC_KAFKA_SO")) {
        if (fromEnv[0] != '\0') {
            return std::string(fromEnv);
        }
    }

    const std::string dir = executableDir();
    if (!dir.empty()) {
        static const char* const relative[] = {
            "/../out64/librdkafka_onec.so",
            "/out64/librdkafka_onec.so",
            "/../../out64/librdkafka_onec.so",
        };
        for (const char* suffix : relative) {
            const std::string candidate = dir + suffix;
            if (isRegularFile(candidate)) {
                return candidate;
            }
        }
    }

    return onec::ComponentLibrary::defaultPath();
}

// dlopen() refcounts, so opening a library the ComponentLibrary already holds is
// free and closing this handle cannot unload it from under the test.
class DlHandle
{
public:
    explicit DlHandle(const std::string& path)
        : handle_(::dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL))
    {
    }

    ~DlHandle()
    {
        if (handle_ != nullptr) {
            ::dlclose(handle_);
        }
    }

    DlHandle(const DlHandle&) = delete;
    DlHandle& operator=(const DlHandle&) = delete;

    void* get() const { return handle_; }
    bool  ok() const { return handle_ != nullptr; }

    template <typename Fn>
    Fn symbol(const char* name) const
    {
        if (handle_ == nullptr) {
            return nullptr;
        }
        ::dlerror();
        return reinterpret_cast<Fn>(::dlsym(handle_, name));
    }

private:
    void* handle_;
};

//---------------------------------------------------------------------------//
// Shared checks
//---------------------------------------------------------------------------//

std::string where(const onec::ComponentObject& object, const char* member)
{
    return object.className() + "::" + member;
}

void checkMethodTable(onec::ComponentObject& object, const ClassSpec& spec)
{
    // Every registered entry point must be accounted for by the table above,
    // otherwise a new method could appear and silently go untested.
    CHECK_EQ(object.methodCount(), static_cast<long>(spec.methodCount));

    for (std::size_t i = 0; i < spec.methodCount; ++i) {
        const MethodSpec& method = spec.methods[i];
        const std::string label  = where(object, method.name);

        const long number = object.findMethod(method.name);
        CHECK_MSG(number >= 0, label + ": FindMethod returned -1");
        if (number < 0) {
            continue;
        }

        const long params = object.paramCount(number);
        CHECK_MSG(params == method.params,
                  label + ": GetNParams == " + std::to_string(params)
                      + ", README.md documents " + std::to_string(method.params));

        // src/*1c.cpp registers everything through AddFunctionProperty with a
        // null procedure pointer, so HasRetVal is true for all of them and 1C
        // code always calls them as functions.
        CHECK_MSG(object.hasRetVal(number), label + ": HasRetVal == false (registered as a procedure?)");

        // The English alias has to round-trip, or FindMethod found the method
        // under its Russian name and the English spelling has drifted.
        CHECK_EQ(object.methodName(number, 0), std::string(method.name));

        // Both languages are registered; the Russian spellings are not asserted
        // one by one, only that none of them is missing.
        CHECK_MSG(!object.methodName(number, 1).empty(), label + ": no Russian alias registered");
    }
}

// Initialize takes a different number of parameters on each class - brokers,
// brokers + group id, brokers + topic + partition - so anything that wants to
// drive all three from one loop goes through here. The first parameter is an
// Arg rather than a string so a caller can also hand it the wrong type on
// purpose.
onec::Value initialize(onec::ComponentObject& object, const onec::Arg& brokers)
{
    const std::string& className = object.className();
    if (className == "KafkaConsumer") {
        return object.callFunc(u"Initialize", { brokers, "onec-smoke-group" });
    }
    if (className == "KafkaProducer") {
        return object.callFunc(u"Initialize", { brokers, "onec-smoke-topic", -1 });
    }
    return object.callFunc(u"Initialize", { brokers });
}

// true when Initialize reported success. Initialize always returns true from
// the dispatch itself (src/consumer1c.cpp:139 sets ret = true unconditionally)
// and carries the outcome in the VTYPE_BOOL return value, so this never throws
// on a failed call - the failure is the bool, not an exception.
bool initializeOk(onec::ComponentObject& object, const onec::Arg& brokers)
{
    const onec::Value result = initialize(object, brokers);
    return result.isBool() && result.asBool();
}

// A garbage broker address makes librdkafka log at levels 3-5 ("Failed to
// resolve", "No brokers configured") from its own threads, straight to stderr,
// which would bury the test output. log_level is a plain global property, so
// silencing it is a one-liner and it changes nothing this file asserts.
void quiet(onec::ComponentObject& object)
{
    CHECK_MSG(object.callBool(u"SetGlobalConf", { "log_level", "0" }),
              object.className() + ": SetGlobalConf(log_level, 0) failed: "
                  + object.errorDescription());
}

void checkPropertyTable(onec::ComponentObject& object, const ClassSpec& spec)
{
    CHECK_EQ(object.propCount(), static_cast<long>(spec.propCount));

    for (std::size_t i = 0; i < spec.propCount; ++i) {
        const char* const  name  = spec.props[i];
        const std::string  label = object.className() + "." + name;

        const long number = object.findProp(name);
        CHECK_MSG(number >= 0, label + ": FindProp returned -1");
        if (number < 0) {
            continue;
        }
        CHECK_EQ(object.propName(number, 0), std::string(name));
        CHECK_MSG(!object.propName(number, 1).empty(), label + ": no Russian alias registered");
    }
}

//---------------------------------------------------------------------------//
// Running a scenario in a child process
//---------------------------------------------------------------------------//

// Some of what this file has to prove is that the component does NOT corrupt
// its own heap - a dangling RdKafka::KafkaConsumer* that ~KafkaConsumerCore1C()
// runs close() and delete on. There is no return value to assert on: either the
// object destructs cleanly or the process dies, and on a component built with
// -fsanitize=address it dies with an ASan report. A crash in-process would take
// the whole smoke run with it and there would be no way to say WHICH scenario
// died, so every such scenario runs in a fork()ed child and the parent turns
// "the child did not come back with status 0" into an ordinary named failure.
//
// fork() is only safe while this process is single-threaded: a librdkafka
// background thread holding the malloc lock at the wrong moment would deadlock
// the child. That is why the case that uses this runs BEFORE anything in this
// file has called Initialize - at that point the component is dlopen()ed and
// nothing has started a thread yet.
struct ChildOutcome
{
    bool exited = false;
    int  status = -1;
    int  signalNumber = 0;

    bool ok() const { return exited && status == 0; }

    std::string describe() const
    {
        if (exited) {
            return "exited with status " + std::to_string(status);
        }
        if (signalNumber != 0) {
            const char* const name = ::strsignal(signalNumber);
            return std::string("killed by signal ") + std::to_string(signalNumber) + " ("
                   + (name != nullptr ? name : "?") + ")";
        }
        return "ended in an unknown way";
    }
};

ChildOutcome runInChild(const std::function<void()>& body)
{
    // The child inherits a copy of this process's stdio buffers; anything not
    // flushed now would be printed twice.
    std::cout.flush();
    std::cerr.flush();

    const int failuresBefore = test::failCount();

    const pid_t pid = ::fork();
    if (pid < 0) {
        throw std::runtime_error(std::string("fork() failed: ") + std::strerror(errno));
    }

    if (pid == 0) {
        int rc = 0;
        try {
            body();   // destructors of everything body() owns run here
        } catch (const std::exception& e) {
            std::cout << "  ...   child: unexpected exception: " << e.what() << std::endl;
            rc = 70;
        } catch (...) {
            std::cout << "  ...   child: unexpected non-standard exception" << std::endl;
            rc = 70;
        }
        if (rc == 0 && test::failCount() != failuresBefore) {
            rc = 1;   // the child printed its own FAIL lines
        }
        std::cout.flush();
        std::cerr.flush();
        // _exit, never exit(): LeakSanitizer's atexit hook would otherwise run
        // in this short-lived child and report librdkafka's own allocations,
        // which the parent process is the one that accounts for.
        ::_exit(rc);
    }

    int status = 0;
    while (::waitpid(pid, &status, 0) < 0) {
        if (errno != EINTR) {
            throw std::runtime_error(std::string("waitpid() failed: ") + std::strerror(errno));
        }
    }

    ChildOutcome outcome;
    if (WIFEXITED(status)) {
        outcome.exited = true;
        outcome.status = WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
        outcome.signalNumber = WTERMSIG(status);
    }
    return outcome;
}

// One scenario: run it in a child, report the outcome as a single check.
void checkScenario(const std::string& name, const std::function<void()>& body)
{
    const ChildOutcome outcome = runInChild(body);
    CHECK_MSG(outcome.ok(), name + ": the child process " + outcome.describe()
                                + " - the scenario must run and the objects must be "
                                  "destroyed without crashing");
}

//---------------------------------------------------------------------------//
// UTF-8 validation, for the strings the component builds itself
//---------------------------------------------------------------------------//

// ErrorDescription crosses the ABI through iconv, so a text that is not
// well-formed UTF-8 cannot survive the trip: the component either truncates it
// to nothing or (since the allocString fix) refuses the property read outright.
// Either way the script loses the diagnostic. This is the check that says the
// component kept the text well-formed while capping it.
bool isValidUtf8(const std::string& text)
{
    std::size_t i = 0;
    while (i < text.size()) {
        const unsigned char lead = static_cast<unsigned char>(text[i]);
        std::size_t         extra = 0;
        unsigned int        code = 0;

        if (lead < 0x80u) {
            ++i;
            continue;
        } else if ((lead & 0xE0u) == 0xC0u) {
            extra = 1;
            code = lead & 0x1Fu;
        } else if ((lead & 0xF0u) == 0xE0u) {
            extra = 2;
            code = lead & 0x0Fu;
        } else if ((lead & 0xF8u) == 0xF0u) {
            extra = 3;
            code = lead & 0x07u;
        } else {
            return false;   // a continuation byte or 0xF8..0xFF as a lead
        }

        if (i + extra >= text.size()) {
            return false;   // the sequence is cut short
        }
        for (std::size_t k = 1; k <= extra; ++k) {
            const unsigned char cont = static_cast<unsigned char>(text[i + k]);
            if ((cont & 0xC0u) != 0x80u) {
                return false;
            }
            code = (code << 6) | (cont & 0x3Fu);
        }
        // Overlong forms, surrogates and anything past U+10FFFF are ill-formed.
        if ((extra == 1 && code < 0x80u) || (extra == 2 && code < 0x800u)
            || (extra == 3 && code < 0x10000u) || (code >= 0xD800u && code <= 0xDFFFu)
            || code > 0x10FFFFu) {
            return false;
        }
        i += extra + 1;
    }
    return true;
}

// 200 x U+0410 (CYRILLIC CAPITAL LETTER A, 2 bytes each) behind a "://" that
// librdkafka can parse no broker out of. 403 bytes in total, and byte 200 -
// where the component's 200-byte cap falls - is 0x90, the second byte of a
// character. A naive substr(0, 200) therefore ends in half a character.
std::string longCyrillicBrokerString()
{
    std::string brokers = "://";
    for (int i = 0; i < 200; ++i) {
        brokers += "\xD0\x90";
    }
    return brokers;
}

} // namespace

//---------------------------------------------------------------------------//
// main
//---------------------------------------------------------------------------//

int main(int argc, char** argv)
{
    const std::string soPath = resolveSoPath(argc, argv);
    test::note("component: " + soPath);

    std::unique_ptr<onec::ComponentLibrary> library;

    //-----------------------------------------------------------------------//
    test::run("the .so loads and exports the 1C entry points", [&] {
        std::string loadError;
        try {
            library.reset(new onec::ComponentLibrary(soPath));
        } catch (const std::exception& e) {
            loadError = e.what();
        }
        // ComponentLibrary's constructor dlopen()s the file and resolves all five
        // exports from src/exports.def, so getting here at all is the check.
        REQUIRE_MSG(library != nullptr, "could not load " + soPath + ": " + loadError);

        // librdkafka leaves background threads and OpenSSL state behind; unmapping
        // the library underneath them at exit buys nothing and can turn a green run
        // into a segfault after the last check. The process is about to end anyway.
        library->setCloseOnDestroy(false);

        CHECK_EQ(library->path(), soPath);

        // GetClassNames() -> u"|KafkaProducer|KafkaConsumer|KafkaAdminClient".
        const std::vector<std::string>& names = library->classNames();
        CHECK_EQ(names.size(), std::size_t(3));
        for (std::size_t i = 0; i < 3; ++i) {
            const std::string actual = i < names.size() ? names[i] : std::string("<missing>");
            CHECK_EQ(actual, std::string(kExpectedClassNames[i]));
        }
        CHECK_TRUE(library->hasClass(u"KafkaProducer"));
        CHECK_TRUE(library->hasClass(u"KafkaConsumer"));
        CHECK_TRUE(library->hasClass(u"KafkaAdminClient"));
        CHECK_FALSE(library->hasClass(u"KafkaSomethingElse"));

        // GetAttachType() -> eCanAttachAny: the component declares it can run
        // both isolated and in-process.
        CHECK_EQ(static_cast<int>(library->attachType()), static_cast<int>(eCanAttachAny));

        // SetPlatformCapabilities() answers with the highest Native API
        // capability level it understands, whatever the platform offers.
        CHECK_EQ(static_cast<int>(library->setPlatformCapabilities(eAppCapabilities1)),
                 static_cast<int>(eAppCapabilitiesLast));
        CHECK_EQ(static_cast<int>(library->setPlatformCapabilities(eAppCapabilities3)),
                 static_cast<int>(eAppCapabilitiesLast));
    });

    if (!library) {
        test::note("no component loaded, the remaining cases cannot run");
        return test::summary("smoke");
    }

    //-----------------------------------------------------------------------//
    test::run("the .so is linked against librdkafka " + std::string(kExpectedRdKafkaVersion), [&] {
        // No 1C method or property reports the librdkafka version - nothing in
        // src/ calls rd_kafka_version() - so the honest source is the library
        // itself. librdkafka is linked statically into librdkafka_onec.so but its
        // C API keeps default visibility, so rd_kafka_version_str() is in the
        // dynamic symbol table and dlsym() reaches it. That is the real function
        // compiled into this binary; there is nothing here a stale or wrong build
        // could satisfy.
        DlHandle handle(soPath);

        // dlerror() clears itself on every read and REQUIRE_MSG evaluates its
        // message expression twice, so the reason has to be captured once, here,
        // into a local that both evaluations can read.
        std::string dlOpenError;
        if (!handle.ok()) {
            const char* const text = ::dlerror();
            dlOpenError = text != nullptr ? text : "(no dlerror)";
        }
        REQUIRE_MSG(handle.ok(), "dlopen(" + soPath + ") failed: " + dlOpenError);

        using VersionStrFn = const char* (*)();
        using VersionFn    = int (*)();

        const VersionStrFn versionStr = handle.symbol<VersionStrFn>("rd_kafka_version_str");
        REQUIRE_MSG(versionStr != nullptr,
                    "rd_kafka_version_str is not exported by " + soPath
                        + " - the version cannot be verified, do not trust the SSL suite");

        const std::string reported = versionStr() != nullptr ? versionStr() : "";
        test::note("librdkafka reports " + reported);
        CHECK_EQ(reported, std::string(kExpectedRdKafkaVersion));

        // The packed integer is a second, independent witness: RD_KAFKA_VERSION
        // is a compile-time constant of the same headers the component was built
        // against, 0xMMmmRRPP.
        const VersionFn version = handle.symbol<VersionFn>("rd_kafka_version");
        CHECK_TRUE(version != nullptr);
        if (version != nullptr) {
            const unsigned int packed = static_cast<unsigned int>(version());
            const std::string  decoded = std::to_string((packed >> 24) & 0xffu) + "."
                                       + std::to_string((packed >> 16) & 0xffu) + "."
                                       + std::to_string((packed >> 8) & 0xffu);
            CHECK_EQ(decoded, std::string(kExpectedRdKafkaVersion));
        }
    });

    //-----------------------------------------------------------------------//
    // This case has to come before anything in this file calls Initialize: it
    // fork()s, and fork() in a process that already has librdkafka's background
    // threads running can deadlock the child on a lock no thread is left to
    // release. Nothing above here has created a client.
    //-----------------------------------------------------------------------//
    test::run("re-Initialize after a successful Initialize, then release the object", [&] {
        // THE BUG THIS GUARDS (review findings 1 and 2).
        //
        // KafkaConsumerCore1C::Initialize() tears the previous client down -
        //     consumer->close(); delete consumer;
        // - and only then checks its arguments. Four checks can return between
        // that delete and the RdKafka::KafkaConsumer::create() that reassigns
        // the member, and the member used to be left pointing at freed memory.
        // ~KafkaConsumerCore1C() then runs close() and delete on it: a
        // use-after-free followed by a double free, in-process, inside the 1C
        // platform. KafkaProducerCore::Initialize() has the same shape.
        //
        // Two things make this easy to miss, and both are the reason the case
        // is written the way it is:
        //
        //   * the failing call RETURNS NORMALLY. Initialize() answers false
        //     with a correct ErrorDescription on the old build too, so a test
        //     that only looks at the return value sees nothing wrong. The
        //     damage surfaces when the object is DESTROYED, which is why every
        //     scenario below ends by letting the ComponentObject go out of
        //     scope rather than leaking it.
        //   * the corruption is a read of freed memory in code the test binary
        //     did not compile, so it is only reported when the COMPONENT itself
        //     is built with -fsanitize=address. Without that it is a crash, and
        //     with a Release component it is usually still a crash, because the
        //     freed block's first word is a vtable pointer the allocator has
        //     since overwritten. Either way the child does not come back with
        //     status 0, which is what checkScenario() asserts.
        //
        // Reachability differs between the two classes and the difference is
        // asserted here rather than assumed:
        //
        //   consumer - the delete sits ABOVE the empty-brokers and empty-group
        //              checks (src/consumer1c_core.cpp:104-131), so an ordinary
        //              1C script reaches it with Initialize("", ...) or
        //              Initialize(addr, "").
        //   producer - the delete sits BELOW the empty-brokers and empty-topic
        //              checks (src/producer1c_core.cpp:92-107); the only early
        //              return left between the delete and the create is a
        //              failing GlobalConfDefaultInit(), and with librdkafka
        //              2.15.1 no 1C-reachable argument makes that fail. The
        //              producer scenarios below therefore pass on the OLD build
        //              too. They are kept because they are the regression guard
        //              for the moment someone adds a sixth early return, and
        //              because a plain re-Initialize must keep working - but
        //              they prove nothing about the fix, and saying so here is
        //              better than leaving them looking like evidence.
        //
        // "127.0.0.1:65533" is a dead port on the loopback interface: rd_kafka_new()
        // hands every socket to librdkafka's own threads, so no scenario waits
        // on the network.
        const char* const kDead  = "127.0.0.1:65533";
        const char* const kDead2 = "127.0.0.1:65534";

        //--- KafkaConsumer: the paths a 1C script actually reaches -----------//

        checkScenario("consumer: Initialize(ok) -> Initialize(\"\") -> release", [&] {
            onec::ComponentObject consumer(*library, u"KafkaConsumer");
            quiet(consumer);
            CHECK_TRUE(initializeOk(consumer, kDead));
            CHECK_EQ(consumer.errorDescription(), std::string("Sucess"));

            // The re-Initialize that used to leave `consumer` dangling. The
            // answer itself is unchanged by the fix and is pinned so a future
            // "fix" that starts rejecting the call earlier shows up here.
            CHECK_FALSE(consumer.callBool(u"Initialize", {"", "onec-smoke-group"}));
            CHECK_EQ(consumer.errorDescription(),
                     std::string("Bad parametrs: empty broker address"));
            CHECK_EQ(consumer.memory().liveBlocks(), std::size_t(0));
        });   // ~ComponentObject -> Done() -> ~KafkaConsumerCore1C() HERE

        checkScenario("consumer: Initialize(ok) -> Initialize(addr, \"\") -> release", [&] {
            onec::ComponentObject consumer(*library, u"KafkaConsumer");
            quiet(consumer);
            CHECK_TRUE(initializeOk(consumer, kDead));
            // An empty group id is the second early return below the delete.
            CHECK_FALSE(consumer.callBool(u"Initialize", {kDead2, ""}));
            CHECK_EQ(consumer.errorDescription(), std::string("Bad parametrs: empty group_id"));
            CHECK_EQ(consumer.memory().liveBlocks(), std::size_t(0));
        });

        checkScenario("consumer: Initialize(ok) -> Initialize(\"\") -> Initialize(ok)", [&] {
            // Not only a teardown bug: on the old build the THIRD call faulted
            // inside Initialize itself, on the consumer->close() at the top,
            // long before any destructor ran. A 1C script that retries after a
            // bad settings constant hits this.
            onec::ComponentObject consumer(*library, u"KafkaConsumer");
            quiet(consumer);
            CHECK_TRUE(initializeOk(consumer, kDead));
            CHECK_FALSE(consumer.callBool(u"Initialize", {"", "onec-smoke-group"}));
            CHECK_MSG(initializeOk(consumer, kDead2),
                      "the consumer did not recover from a failed re-Initialize: "
                          + consumer.errorDescription());
            CHECK_EQ(consumer.errorDescription(), std::string("Sucess"));
            CHECK_EQ(consumer.memory().liveBlocks(), std::size_t(0));
        });

        checkScenario("consumer: Initialize(ok) -> Initialize(\"://\") -> release", [&] {
            // The zero-broker path. It already nulled the member before the fix
            // (src/consumer1c_core.cpp, the rd_kafka_brokers_add == 0 branch),
            // so this one is a pin of behaviour that was always correct, not a
            // guard - and it is here so a future rework of that branch cannot
            // quietly lose the nulling.
            onec::ComponentObject consumer(*library, u"KafkaConsumer");
            quiet(consumer);
            CHECK_TRUE(initializeOk(consumer, kDead));
            CHECK_FALSE(initializeOk(consumer, "://"));
            CHECK_CONTAINS(consumer.errorDescription(), "no usable broker address");
            CHECK_EQ(consumer.memory().liveBlocks(), std::size_t(0));
        });

        checkScenario("consumer: Initialize(ok) -> Initialize(ok) -> release", [&] {
            // The success path through the same delete. Must stay working.
            onec::ComponentObject consumer(*library, u"KafkaConsumer");
            quiet(consumer);
            CHECK_TRUE(initializeOk(consumer, kDead));
            CHECK_TRUE(initializeOk(consumer, kDead2));
            CHECK_EQ(consumer.errorDescription(), std::string("Sucess"));
            CHECK_EQ(consumer.memory().liveBlocks(), std::size_t(0));
        });

        //--- KafkaProducer: latent today, see the note above -----------------//

        checkScenario("producer: Initialize(ok) -> Initialize(\"\") -> release", [&] {
            onec::ComponentObject producer(*library, u"KafkaProducer");
            quiet(producer);
            CHECK_TRUE(initializeOk(producer, kDead));
            CHECK_FALSE(producer.callBool(u"Initialize", {"", "onec-smoke-topic", -1}));
            CHECK_EQ(producer.errorDescription(),
                     std::string("Bad parametrs: empty broker address"));
            CHECK_EQ(producer.memory().liveBlocks(), std::size_t(0));
        });

        checkScenario("producer: Initialize(ok) -> Initialize(addr, \"\") -> release", [&] {
            onec::ComponentObject producer(*library, u"KafkaProducer");
            quiet(producer);
            CHECK_TRUE(initializeOk(producer, kDead));
            CHECK_FALSE(producer.callBool(u"Initialize", {kDead2, "", -1}));
            CHECK_EQ(producer.errorDescription(), std::string("Bad parametrs: empty topic"));
            CHECK_EQ(producer.memory().liveBlocks(), std::size_t(0));
        });

        checkScenario("producer: Initialize(ok) -> Initialize(\"://\") -> release", [&] {
            onec::ComponentObject producer(*library, u"KafkaProducer");
            quiet(producer);
            CHECK_TRUE(initializeOk(producer, kDead));
            CHECK_FALSE(initializeOk(producer, "://"));
            CHECK_CONTAINS(producer.errorDescription(), "no usable broker address");
            CHECK_EQ(producer.memory().liveBlocks(), std::size_t(0));
        });

        checkScenario("producer: Initialize(ok) -> Initialize(ok) -> release", [&] {
            onec::ComponentObject producer(*library, u"KafkaProducer");
            quiet(producer);
            CHECK_TRUE(initializeOk(producer, kDead));
            CHECK_TRUE(initializeOk(producer, kDead2));
            CHECK_EQ(producer.errorDescription(), std::string("Sucess"));
            CHECK_EQ(producer.memory().liveBlocks(), std::size_t(0));
        });

        //--- KafkaAdminClient: the class that was already right --------------//

        checkScenario("admin: Initialize(ok) -> Initialize(\"\") -> release", [&] {
            // KafkaAdminClientCore has always nulled every handle it destroys;
            // the review confirmed it and this pins it.
            onec::ComponentObject admin(*library, u"KafkaAdminClient");
            quiet(admin);
            CHECK_TRUE(initializeOk(admin, kDead));
            CHECK_FALSE(initializeOk(admin, ""));
            CHECK_EQ(admin.errorDescription(), std::string("Bad parametrs: empty broker address"));
            CHECK_TRUE(initializeOk(admin, kDead2));
            CHECK_EQ(admin.memory().liveBlocks(), std::size_t(0));
        });

        //--- the same sequences with ConfReset in the middle -----------------//

        checkScenario("consumer: Initialize(ok) -> ConfReset -> Initialize(ok) -> release", [&] {
            // ConfReset deletes conf and tconf. RdKafka::Conf::create() only
            // fails on an allocation failure, so the dangling members it used to
            // leave behind are not reachable from 1C either - this is a pin of
            // the ordinary path, not a guard for that.
            onec::ComponentObject consumer(*library, u"KafkaConsumer");
            quiet(consumer);
            CHECK_TRUE(initializeOk(consumer, kDead));
            CHECK_TRUE(consumer.callBool(u"ConfReset"));
            quiet(consumer);
            CHECK_TRUE(initializeOk(consumer, kDead2));
            CHECK_EQ(consumer.memory().liveBlocks(), std::size_t(0));
        });
    });

    //-----------------------------------------------------------------------//
    for (const ClassSpec& spec : kClasses) {
        test::run(std::string(spec.className) + ": lifecycle, methods and properties", [&] {
            // ComponentObject's constructor is the platform's sequence:
            // GetClassObject -> Init -> setMemManager -> RegisterExtensionAs.
            std::unique_ptr<onec::ComponentObject> object;
            std::string                            createError;
            try {
                object.reset(new onec::ComponentObject(*library, spec.className));
            } catch (const std::exception& e) {
                createError = e.what();
            }
            REQUIRE_MSG(object != nullptr,
                        std::string("could not create ") + spec.className + ": " + createError);

            // RegisterExtensionAs hands back the name 1C will expose the object
            // under; ComponentBase seeds it from the constructor argument.
            CHECK_EQ(object->registeredName(), std::string(spec.className));

            // Native API 2.0.
            CHECK_EQ(object->info(), 2000L);

            checkMethodTable(*object, spec);
            checkPropertyTable(*object, spec);

            // A method that was never registered, and a name that only differs in
            // case, both have to miss.
            CHECK_EQ(object->findMethod("NoSuchMethodAtAll"), -1L);
            CHECK_EQ(object->findProp("NoSuchPropertyAtAll"), -1L);

            // ErrorDescription on a freshly built object. Note the spelling: the
            // component's own success text is "Sucess" (src/errors.cpp:33), not
            // an empty string.
            CHECK_EQ(object->errorDescription(), std::string("Sucess"));
            if (std::string(spec.className) == "KafkaConsumer") {
                CHECK_FALSE(object->fatalError());
            }

            // Nothing should have been reported to the platform just by existing.
            CHECK_EQ(object->host().errorCount(), std::size_t(0));
            CHECK_EQ(object->host().eventCount(), std::size_t(0));

            // Every string the component allocated (method names, property names,
            // ErrorDescription) went back through the memory manager.
            CHECK_EQ(object->memory().liveBlocks(), std::size_t(0));
            CHECK_TRUE(object->memory().totalBlocks() > 0);

            // Done() + DestroyObject() run here; a crash on teardown fails the run.
            object.reset();
        });
    }

    //-----------------------------------------------------------------------//
    test::run("the librdkafka build understands TLS", [&] {
        // rd_kafka_conf_set() rejects security.protocol=ssl at configuration time
        // when librdkafka was built without OpenSSL ("... not supported in this
        // build"). No socket, no broker, no certificate is involved - this is a
        // pure check that the .so carries the TLS code the SSL suite needs.
        onec::ComponentObject consumer(*library, u"KafkaConsumer");

        CHECK_TRUE(consumer.callBool(u"SetGlobalConf", {"security.protocol", "ssl"}));
        CHECK_EQ(consumer.errorDescription(), std::string("Sucess"));

        // An SSL-only property; it does not exist in a build without OpenSSL.
        CHECK_TRUE(consumer.callBool(u"SetGlobalConf", {"ssl.endpoint.identification.algorithm", "https"}));
        CHECK_EQ(consumer.errorDescription(), std::string("Sucess"));

        // And the same through the producer, which owns its own RdKafka::Conf.
        onec::ComponentObject producer(*library, u"KafkaProducer");
        CHECK_TRUE(producer.callBool(u"SetGlobalConf", {"security.protocol", "ssl"}));
        CHECK_EQ(producer.errorDescription(), std::string("Sucess"));

        CHECK_EQ(consumer.memory().liveBlocks(), std::size_t(0));
        CHECK_EQ(producer.memory().liveBlocks(), std::size_t(0));
    });

    //-----------------------------------------------------------------------//
    test::run("bad calls fail predictably instead of crashing", [&] {
        onec::ComponentObject consumer(*library, u"KafkaConsumer");

        // --- unknown method ------------------------------------------------
        // FindMethod is the only guard the platform has, and it returns -1
        // rather than throwing. The harness turns that into a HostError so a
        // typo in a test cannot quietly call method 0.
        CHECK_EQ(consumer.findMethod(u"NoSuchMethodAtAll"), -1L);
        CHECK_THROWS_AS(consumer.callFunc(u"NoSuchMethodAtAll"), onec::HostError);
        CHECK_THROWS_AS(consumer.getProp(u"NoSuchPropertyAtAll"), onec::HostError);

        // --- wrong arity ---------------------------------------------------
        // ComponentBase::CallAsFunc ignores lSizeArray and the method bodies index
        // paParams unconditionally (Consumer1C::Initialize reads paParams and
        // paParams + 1 before anything else), so handing the component a short
        // parameter array is undefined behaviour, not a graceful error. The 1C
        // platform never does that - it reads GetNParams first - and neither does
        // the harness: it refuses the call. That refusal is the predictable
        // failure worth asserting; deliberately corrupting the process to watch it
        // die would prove nothing about the build.
        CHECK_THROWS_AS(consumer.callFunc(u"Initialize", {"broker:9093"}), onec::HostError);
        CHECK_THROWS_AS(consumer.callFunc(u"Initialize", {"broker:9093", "group", "extra"}),
                        onec::HostError);
        CHECK_THROWS_AS(consumer.callFunc(u"ConfReset", {"unexpected"}), onec::HostError);

        // Surplus parameters straight into the component are safe - the array is
        // longer than the method reads - and must simply be ignored.
        {
            const long confReset = consumer.findMethod(u"ConfReset");
            REQUIRE(confReset >= 0);

            tVariant retValue;
            onec::varInit(retValue);
            tVariant params[3];
            for (tVariant& param : params) {
                onec::varInit(param);
            }
            CHECK_TRUE(consumer.raw()->CallAsFunc(confReset, &retValue, params, 3));
            CHECK_EQ(static_cast<int>(retValue.vt), static_cast<int>(VTYPE_BOOL));
            onec::varFree(retValue, consumer.memory());
        }

        // --- method index out of range -------------------------------------
        // ComponentBase bounds-checks the upper end of the index, so the platform
        // handing over a stale method number returns false rather than jumping
        // through a garbage std::function.
        {
            const long bogus = consumer.methodCount() + 1000;
            tVariant   retValue;
            onec::varInit(retValue);
            tVariant params[1];
            onec::varInit(params[0]);

            CHECK_FALSE(consumer.raw()->CallAsFunc(bogus, &retValue, params, 0));
            CHECK_FALSE(consumer.raw()->CallAsProc(bogus, params, 0));
            CHECK_EQ(consumer.raw()->GetNParams(bogus), 0L);
            CHECK_FALSE(consumer.raw()->HasRetVal(bogus));
            CHECK_TRUE(consumer.raw()->GetMethodName(bogus, 0) == nullptr);

            onec::varFree(retValue, consumer.memory());
        }

        // --- wrong argument types ------------------------------------------
        // The right arity with the wrong tVariant types: the component type-checks
        // and reports through ErrorDescription, it does not throw and does not
        // dereference the bogus pointer.
        CHECK_FALSE(consumer.callBool(u"SetGlobalConf", {1, 2}));
        CHECK_CONTAINS(consumer.errorDescription(), "Bad parametrs");

        // --- a configuration key librdkafka does not know -------------------
        // Proves the conf layer is actually wired to librdkafka rather than
        // swallowing everything, and does it without a broker.
        CHECK_FALSE(consumer.callBool(u"SetGlobalConf", {"bogus.key.that.does.not.exist", "1"}));
        CHECK_CONTAINS(consumer.errorDescription(), "No such configuration property");

        // An empty value is rejected by the component itself, before librdkafka.
        CHECK_FALSE(consumer.callBool(u"SetGlobalConf", {"client.id", ""}));
        CHECK_CONTAINS(consumer.errorDescription(), "Bad parametrs");

        // None of the above reached the platform's error channel, and none of it
        // leaked.
        CHECK_EQ(consumer.host().errorCount(), std::size_t(0));
        CHECK_EQ(consumer.memory().liveBlocks(), std::size_t(0));
    });

    //-----------------------------------------------------------------------//
    test::run("Initialize fails on bad arguments without blocking on the network", [&] {
        // Nothing in this case connects to anything. rd_kafka_new() only builds
        // the client object and hands every socket to librdkafka's own threads,
        // so Initialize returns as soon as the configuration has been parsed -
        // whether a broker exists at the address, or the address is nonsense,
        // makes no difference to how long it takes. The elapsed time is measured
        // at the end and asserted, because "does not hang" is the whole point of
        // running these cases in a broker-free binary.
        const auto started = std::chrono::steady_clock::now();

        for (const ClassSpec& spec : kClasses) {
            //--- an empty broker list -------------------------------------//
            // Rejected by the component itself, before librdkafka. All three
            // classes now report this identically - the consumer and the admin
            // client used to say ERR_UNHANDLED "brokers addresses is empty"
            // while the producer said ERR_BADPARAMETR "empty broker address";
            // they were unified on the producer's wording, which carries the
            // right error code for a caller mistake. The exact string is
            // asserted below so a future divergence shows up here.
            {
                onec::ComponentObject object(*library, spec.className);
                quiet(object);

                const onec::Value result = initialize(object, "");
                CHECK_EQ(static_cast<int>(result.type()), static_cast<int>(VTYPE_BOOL));
                CHECK_FALSE(result.isBool() && result.asBool());

                const std::string error = object.errorDescription();
                CHECK_MSG(!error.empty(),
                          std::string(spec.className)
                              + ": Initialize(\"\") left ErrorDescription empty");
                CHECK_CONTAINS(error, "broker");
                CHECK_CONTAINS(error, "empty");
                CHECK_MSG(error == "Bad parametrs: empty broker address",
                          std::string(spec.className)
                              + ": Initialize(\"\") -> \"" + error
                              + "\", expected all three classes to agree on "
                                "\"Bad parametrs: empty broker address\"");

                CHECK_EQ(object.host().errorCount(), std::size_t(0));
                CHECK_EQ(object.memory().liveBlocks(), std::size_t(0));
            }

            //--- a broker argument of the wrong tVariant type ---------------//
            // The arity is right, the type is not. Initialize dereferences
            // pwstrVal only after checking vt, so this has to come back as a
            // reported error rather than a crash.
            {
                onec::ComponentObject object(*library, spec.className);
                quiet(object);

                CHECK_FALSE(initializeOk(object, 42));
                CHECK_CONTAINS(object.errorDescription(), "Bad parametrs");
                CHECK_EQ(object.host().errorCount(), std::size_t(0));
                CHECK_EQ(object.memory().liveBlocks(), std::size_t(0));
            }
        }

        //--- the per-class second argument ---------------------------------//
        {
            onec::ComponentObject consumer(*library, u"KafkaConsumer");
            quiet(consumer);
            CHECK_FALSE(consumer.callBool(u"Initialize", { "127.0.0.1:65533", "" }));
            // Was ERR_UNHANDLED "group_id is empty" in Initialize while
            // GlobalConfDefaultInit five lines away used ERR_BADPARAMETR
            // "empty group_id" for the same condition; unified on the latter.
            CHECK_CONTAINS(consumer.errorDescription(), "empty group_id");
            CHECK_CONTAINS(consumer.errorDescription(), "Bad parametrs");
            CHECK_EQ(consumer.memory().liveBlocks(), std::size_t(0));
        }
        {
            onec::ComponentObject producer(*library, u"KafkaProducer");
            quiet(producer);
            CHECK_FALSE(producer.callBool(u"Initialize", { "127.0.0.1:65533", "", -1 }));
            CHECK_CONTAINS(producer.errorDescription(), "empty topic");
            CHECK_EQ(producer.memory().liveBlocks(), std::size_t(0));
        }

        //--- broker strings librdkafka cannot parse a broker out of ---------//
        // These used to be ACCEPTED. The component passed the string to
        // librdkafka as metadata.broker.list, librdkafka parsed what it could,
        // logged "parse error" / "No brokers configured" at log level 3-5 on a
        // channel the component never reads, and still handed back a usable
        // -looking client - so Initialize reported success and the mistake only
        // surfaced later as a connection that never happens.
        //
        // The component now asks librdkafka for its own verdict: right after
        // the handle exists it calls rd_kafka_brokers_add(handle, brokers),
        // which re-runs rd_kafka_broker_name_parse over the same list and
        // returns how many brokers came out of it (re-adding an identical list
        // is idempotent - the already-configured broker is counted, not
        // duplicated). Zero means librdkafka derived no broker at all, which is
        // exactly the state the empty-string case above already rejected, so
        // this is a generalisation of an existing rule and not a new syntax
        // grammar invented by the component.
        //
        // Each of these yields zero brokers, on all three classes.
        static const char* const kRejectedBrokers[] = {
            "://",                      // empty protocol name
            "tcp://[::1",               // unsupported protocol, unbalanced bracket
            ",",                        // a separator and nothing else
            " ",                        // one space
            "http://example.com/path",  // a URL, not a broker list
        };

        for (const ClassSpec& spec : kClasses) {
            for (const char* const brokers : kRejectedBrokers) {
                onec::ComponentObject object(*library, spec.className);
                quiet(object);

                const std::string what = std::string(spec.className)
                                             + ": Initialize(\"" + brokers + "\")";
                CHECK_MSG(!initializeOk(object, brokers),
                          what + " was accepted - librdkafka parses no broker "
                                 "out of it, so it must be rejected");

                const std::string error = object.errorDescription();
                CHECK_MSG(error.find("no usable broker address") != std::string::npos,
                          what + " -> \"" + error
                              + "\", expected \"no usable broker address\"");
                // The offending string is quoted back, capped at 200 chars by
                // the component - that is what makes the error debuggable from
                // 1C, so it is asserted rather than left to chance.
                CHECK_MSG(error.find(brokers) != std::string::npos,
                          what + " -> \"" + error
                              + "\" does not quote the offending string back");

                CHECK_EQ(object.host().errorCount(), std::size_t(0));
                CHECK_EQ(object.memory().liveBlocks(), std::size_t(0));

                // ~ComponentObject -> Done() here, on top of the teardown the
                // reject path already did. Destroying twice must be harmless.
            }
        }

        //--- odd-looking broker strings that are still accepted -------------//
        // Deliberately NOT rejected, and this is the deliberate part: each of
        // these makes librdkafka produce a real broker entry, so refusing them
        // would mean the component inventing a hostname grammar of its own and
        // breaking 1C code that works today.
        //
        //   "!!!"                -> host "!!!", port 9092. Indistinguishable
        //                           from a typo'd but ordinary internal
        //                           hostname; it fails loudly at DNS time.
        //   "localhost:notaport" -> host "localhost", port atoi("notaport") = 0.
        //                           Rejecting it would mean duplicating
        //                           rd_kafka_broker_name_parse's IPv6-vs-port
        //                           disambiguation, which would drift on the
        //                           next librdkafka bump; it fails loudly every
        //                           reconnect with "Connect to ...:0 failed".
        //   "kafka_prod_01:9092" -> an underscore in a hostname is real and
        //                           works; a naive grammar would have killed it.
        //   "::1", "[::1]:9092"  -> IPv6, both legitimate.
        //   "localhost:9092,://" -> one element parses, so there IS a broker. A
        //                           partly fat-fingered list is not fatal.
        static const char* const kAcceptedBrokers[] = {
            "!!!",
            "localhost:notaport",
            "kafka_prod_01:9092",
            "::1",
            "[::1]:9092",
            "localhost:9092,://",
        };

        for (const ClassSpec& spec : kClasses) {
            for (const char* const brokers : kAcceptedBrokers) {
                onec::ComponentObject object(*library, spec.className);
                quiet(object);

                CHECK_MSG(initializeOk(object, brokers),
                          std::string(spec.className) + ": Initialize(\"" + brokers
                              + "\") now fails - librdkafka still parses a broker "
                                "out of it, so the component must not refuse it: "
                              + object.errorDescription());
                CHECK_EQ(object.errorDescription(), std::string("Sucess"));
                CHECK_EQ(object.host().errorCount(), std::size_t(0));
                CHECK_EQ(object.memory().liveBlocks(), std::size_t(0));

                // ~ComponentObject -> Done() -> rd_kafka_destroy() here, with
                // librdkafka's threads already running against an address that
                // will never answer.
            }
        }

        const double elapsedMs = std::chrono::duration<double, std::milli>(
                                     std::chrono::steady_clock::now() - started).count();
        test::note("Initialize argument checks took "
                   + std::to_string(static_cast<long>(elapsedMs)) + " ms");
        CHECK_MSG(elapsedMs < 30000.0,
                  "Initialize took " + std::to_string(static_cast<long>(elapsedMs))
                      + " ms - it must never wait on the network");
    });

    //-----------------------------------------------------------------------//
    test::run("a long non-ASCII broker string still produces a readable ErrorDescription", [&] {
        // THE BUG THIS GUARDS (review finding 8).
        //
        // BrokersForMessage() caps the broker string it quotes back at 200
        // BYTES. It used to cut with a plain substr(0, 200), so a non-ASCII
        // address longer than that ended in half a UTF-8 character. The finished
        // message then goes through allocString() -> iconv, iconv stops with
        // EILSEQ on the truncated sequence, and the whole ErrorDescription came
        // back EMPTY. The diagnostic destroyed itself exactly when a non-ASCII
        // broker string was the thing that had gone wrong: Initialize returned
        // false and the 1C script had nothing at all to show for it.
        //
        // The cut now steps back over UTF-8 continuation bytes (at most three)
        // so it always lands on a character boundary.
        const std::string brokers = longCyrillicBrokerString();
        CHECK_EQ(brokers.size(), std::size_t(403));
        // The byte the old cut fell on: 0x90, a continuation byte.
        CHECK_EQ(static_cast<int>(static_cast<unsigned char>(brokers[200])), 0x90);

        for (const ClassSpec& spec : kClasses) {
            onec::ComponentObject object(*library, spec.className);
            quiet(object);

            const std::string what = std::string(spec.className) + " with a 403-byte Cyrillic "
                                                                   "broker string";
            CHECK_MSG(!initializeOk(object, brokers), what + " was accepted");

            const std::string error = object.errorDescription();
            // The guard. On the old build this is "" for all three classes.
            CHECK_MSG(!error.empty(),
                      what + ": ErrorDescription is EMPTY - the 200-byte cut landed inside a "
                             "character and iconv threw the whole message away");
            CHECK_MSG(error.find("no usable broker address") != std::string::npos,
                      what + ": ErrorDescription does not name the problem: \"" + error + "\"");
            CHECK_MSG(isValidUtf8(error),
                      what + ": ErrorDescription is not well-formed UTF-8");

            // The echoed prefix has to be a whole number of characters. The cut
            // starts at 200 and walks back at most three bytes, so 197..200
            // bytes of the original survive; for this particular string it is
            // 199 ("://" plus 98 whole Cyrillic characters). Asserting the range
            // rather than the exact number keeps the check honest if the cap
            // ever moves.
            const std::string opening = "no usable broker address in \"";
            const std::size_t at = error.find(opening);
            const std::size_t from = at == std::string::npos ? 0 : at + opening.size();
            const std::size_t dots =
                at == std::string::npos ? std::string::npos : error.find("...\"", from);
            // Not a REQUIRE: the loop has to report all three classes, and on a
            // build where the description is empty every class is interesting.
            CHECK_MSG(dots != std::string::npos,
                      what + ": the echoed broker string is not present and marked as truncated: \""
                          + error + "\"");
            if (dots == std::string::npos) {
                continue;
            }
            const std::size_t kept = dots - from;
            test::note(std::string(spec.className) + ": echoed " + std::to_string(kept)
                       + " byte(s) of a 403-byte broker string");
            CHECK_MSG(kept >= 197 && kept <= 200,
                      what + ": kept " + std::to_string(kept)
                          + " bytes, expected 197..200 (a whole number of characters below the "
                            "200-byte cap)");
            CHECK_MSG(brokers.compare(0, kept, error, from, kept) == 0,
                      what + ": the echoed prefix is not the head of the argument");
            CHECK_MSG(isValidUtf8(brokers.substr(0, kept)),
                      what + ": the echoed prefix ends inside a character");

            CHECK_EQ(object.host().errorCount(), std::size_t(0));
            CHECK_EQ(object.memory().liveBlocks(), std::size_t(0));
        }

        // A 403-byte ASCII string is cut at exactly 200: the walk-back only ever
        // moves over continuation bytes, so nothing changed for ASCII.
        {
            onec::ComponentObject object(*library, u"KafkaConsumer");
            quiet(object);
            const std::string asciiBrokers = "://" + std::string(400, 'a');
            CHECK_FALSE(initializeOk(object, asciiBrokers));
            const std::string error = object.errorDescription();
            CHECK_CONTAINS(error, "no usable broker address in \"://" + std::string(197, 'a')
                                      + "...\"");
        }
    });

    //-----------------------------------------------------------------------//
    test::run("a partly valid broker list: what the component ACTUALLY does", [&] {
        // This case PINS WHAT THE COMPONENT DOES. It is deliberately not what a
        // reader might wish it did, and the assertions should be read as
        // documentation of a sharp edge rather than as approval of it.
        //
        // The component's only broker-list check is rd_kafka_brokers_add(),
        // which re-runs librdkafka's own parser over the list and returns HOW
        // MANY brokers came out of it; the list is refused only when that count
        // is zero. librdkafka's loop (rd_kafka_brokers_add0) BREAKS at the first
        // element it cannot parse and returns the count it has reached so far.
        // Two consequences, both measured here rather than assumed:
        //
        //   * "good:9092,@@@"  -> at least one broker, so ACCEPTED, reported as
        //     plain "Sucess". The unusable element is dropped by librdkafka and
        //     NOBODY is told - not the component, not the 1C script. A typo in
        //     the middle of a bootstrap list is invisible until the connection
        //     to that particular broker simply never happens. Worse: because the
        //     loop breaks rather than skips, every element AFTER the bad one is
        //     dropped as well, so "good,://,also-good" silently runs on one
        //     broker instead of two.
        //   * "://,good:9092"  -> the bad element comes FIRST, the loop breaks
        //     before counting anything, the count is zero and the whole list is
        //     REJECTED - even though it names a perfectly good broker. The same
        //     two elements in the other order behave in opposite ways.
        //
        // Validation therefore only catches the all-bad case, plus the case
        // where the first element is bad. The alternative - the component
        // inventing a hostname grammar of its own - is worse: "@@@" and
        // "kafka_prod_01" are indistinguishable from an ordinary internal
        // hostname that resolves perfectly well on the customer's network.
        //
        // If any of these verdicts changes, it is a change 1C scripts can see,
        // and the failure here is the notification.

        // Accepted: at least one element parses BEFORE the first bad one.
        static const char* const kAccepted[] = {
            "127.0.0.1:65533,@@@",                      // "@@@" parses as a host
            "@@@,127.0.0.1:65533",                      // ...so either order is fine
            "127.0.0.1:65533,://",                      // good first, unparseable second
            "127.0.0.1:65533,tcp://[::1",               // good first, unsupported protocol second
            "127.0.0.1:65533,http://example.com/path",  // good first, a URL second
            "127.0.0.1:65533,,",                        // empty elements are skipped
            ",127.0.0.1:65533",                         // ...including a leading one
            "127.0.0.1:65533, ",                        // and a blank one
        };

        // Rejected: the FIRST element is one librdkafka cannot parse, so the
        // count is zero and the good element behind it is never reached.
        static const char* const kRejected[] = {
            "://,127.0.0.1:65533",
            "tcp://[::1,127.0.0.1:65533",
        };

        for (const ClassSpec& spec : kClasses) {
            for (const char* const brokers : kAccepted) {
                onec::ComponentObject object(*library, spec.className);
                quiet(object);

                const std::string what = std::string(spec.className) + ": Initialize(\"" + brokers
                                         + "\")";
                CHECK_MSG(initializeOk(object, brokers),
                          what + " was REJECTED. The component only refuses a list librdkafka "
                                 "derives zero brokers from, and this one yields at least one: "
                               + object.errorDescription());
                // No warning, no note, nothing: success is reported exactly as
                // it is for a perfectly spelled list.
                CHECK_MSG(object.errorDescription() == std::string("Sucess"),
                          what + " -> ErrorDescription \"" + object.errorDescription()
                              + "\", expected the plain success text - a partly valid list is "
                                "not reported as a problem anywhere");
                CHECK_EQ(object.host().errorCount(), std::size_t(0));
                CHECK_EQ(object.memory().liveBlocks(), std::size_t(0));
            }

            for (const char* const brokers : kRejected) {
                onec::ComponentObject object(*library, spec.className);
                quiet(object);

                const std::string what = std::string(spec.className) + ": Initialize(\"" + brokers
                                         + "\")";
                CHECK_MSG(!initializeOk(object, brokers),
                          what + " was ACCEPTED. librdkafka's parse loop breaks at the first "
                                 "unparseable element, so a list that begins with one yields "
                                 "zero brokers and must be refused");
                CHECK_CONTAINS(object.errorDescription(), "no usable broker address");
                CHECK_EQ(object.memory().liveBlocks(), std::size_t(0));
            }
        }
    });

    //-----------------------------------------------------------------------//
    test::run("a refused AllocMemory is reported, not silently turned into an empty string", [&] {
        // THE BUG THIS GUARDS (review finding 4).
        //
        // ComponentBase::allocString() used to return void. Every one of its
        // failure modes - no memory manager, AllocMemory refusing, a text that
        // is not valid UTF-8 - left the caller's tVariant as VTYPE_EMPTY or as
        // an empty VTYPE_PWSTR and told nobody. The 1C script then read an empty
        // string together with ErrorDescription == "Sucess", which is
        // indistinguishable from "there was nothing to return".
        //
        // For ReceiveJSONMessages / ReceiveOnecInternalMessages that is data
        // loss, not just a bad message: when
        // RemoveMessagesFromLocalQueueOnJSONBuild is set the builder has already
        // deleted every message it serialised by the time the string is handed
        // over, so the whole batch is gone and the call reported success.
        //
        // A real out-of-memory condition cannot be provoked, but the platform
        // refusing one AllocMemory is the same code path and needs no broker.
        // MemoryManager::failNextAllocations() (tests/host/onec_host.h) arms
        // exactly one refusal.
        //
        // The other half of finding 4 - an ill-formed UTF-8 PAYLOAD - needs a
        // real message, and lives in kafka_ssl_test.cpp.

        //--- the message pool ------------------------------------------------//
        // No Initialize is needed: ReceiveJSONMessages serialises the local
        // queue, which is empty, into "[]" and hands that to allocString.
        for (const char* const method : {"ReceiveJSONMessages", "ReceiveOnecInternalMessages"}) {
            onec::ComponentObject consumer(*library, u"KafkaConsumer");

            consumer.memory().failNextAllocations(1);
            const onec::Value pool = consumer.callFunc(method, {false});
            CHECK_EQ(consumer.memory().pendingAllocFailures(), std::size_t(0));
            CHECK_MSG(consumer.memory().refusedAllocations() == 1,
                      std::string(method) + ": the refusal was never reached");

            // The variant still has to be something the platform can read.
            CHECK_MSG(pool.isEmpty() || (pool.isString() && pool.asString().empty()),
                      std::string(method) + ": returned " + pool.describe()
                          + ", expected VTYPE_EMPTY or an empty string");

            const std::string error = consumer.errorDescription();
            CHECK_MSG(error != std::string("Sucess"),
                      std::string(method) + ": AllocMemory was refused and the call still "
                                            "reported success - a 1C script cannot tell this "
                                            "from an empty poll");
            CHECK_CONTAINS(error, "cannot return the message pool to 1C");
            CHECK_CONTAINS(error, "AllocMemory failed");
            // The text has to tell the script what to do about it.
            CHECK_CONTAINS(error, "base64");

            CHECK_EQ(consumer.memory().foreignFrees(), std::size_t(0));
            CHECK_EQ(consumer.memory().liveBlocks(), std::size_t(0));
        }

        //--- the ErrorDescription property itself ----------------------------//
        // When the description cannot be handed over, GetPropVal now returns
        // FALSE. It used to return true with vt == VTYPE_EMPTY, i.e. Undefined -
        // a silent "no error" on the one property a script reads while it is
        // already handling an error. The harness turns a false GetPropVal into a
        // HostError, which is what the throw below is.
        for (const ClassSpec& spec : kClasses) {
            onec::ComponentObject object(*library, spec.className);

            object.memory().failNextAllocations(1);
            CHECK_THROWS_AS(object.getProp(u"ErrorDescription"), onec::HostError);
            CHECK_MSG(object.memory().refusedAllocations() == 1,
                      std::string(spec.className)
                          + ": reading ErrorDescription did not even try to allocate");

            // The next read, with no refusal armed, works normally: the failure
            // is not sticky.
            CHECK_EQ(object.errorDescription(), std::string("Sucess"));
            CHECK_EQ(object.memory().foreignFrees(), std::size_t(0));
            CHECK_EQ(object.memory().liveBlocks(), std::size_t(0));
        }

        //--- a JSON-returning method that loses nothing ----------------------//
        // Nothing is lost here - the delivery records survive the call, so the
        // report can be asked for again - but an empty string with no error
        // reads in 1C as an empty report, so this reports too.
        // GetJSONDeliveryReport needs neither a broker nor an Initialize: with
        // no records it serialises an empty array.
        {
            onec::ComponentObject producer(*library, u"KafkaProducer");

            producer.memory().failNextAllocations(1);
            const onec::Value report = producer.callFunc(u"GetJSONDeliveryReport");
            CHECK_MSG(producer.memory().refusedAllocations() == 1,
                      "GetJSONDeliveryReport: the refusal was never reached");
            CHECK_MSG(report.isEmpty() || (report.isString() && report.asString().empty()),
                      "GetJSONDeliveryReport returned " + report.describe());

            const std::string error = producer.errorDescription();
            CHECK_MSG(error != std::string("Sucess"),
                      "GetJSONDeliveryReport: AllocMemory was refused and the call still "
                      "reported success");
            CHECK_CONTAINS(error, "cannot return the delivery report to 1C");
            CHECK_CONTAINS(error, "AllocMemory failed");
            CHECK_EQ(producer.memory().foreignFrees(), std::size_t(0));
            CHECK_EQ(producer.memory().liveBlocks(), std::size_t(0));
        }
    });

    //-----------------------------------------------------------------------//
    test::run("SetGlobalConf is really talking to librdkafka on all three classes", [&] {
        // The producer and the consumer go through RdKafka::Conf::set(), the
        // admin client through rd_kafka_conf_set() - two different code paths in
        // src/, so all three are driven here rather than the consumer alone.
        for (const ClassSpec& spec : kClasses) {
            onec::ComponentObject object(*library, spec.className);

            // A key no librdkafka release has ever had. The rejection, and the
            // fact that the key itself is quoted back, is what proves the value
            // reached librdkafka instead of being swallowed by the component.
            CHECK_FALSE(object.callBool(u"SetGlobalConf", { "onec.smoke.no.such.property", "1" }));
            CHECK_CONTAINS(object.errorDescription(), "No such configuration property");
            CHECK_CONTAINS(object.errorDescription(), "onec.smoke.no.such.property");

            // A real key with a value it cannot parse: the value side is checked
            // too, not only the name.
            CHECK_FALSE(object.callBool(u"SetGlobalConf", { "socket.timeout.ms", "not-a-number" }));
            CHECK_CONTAINS(object.errorDescription(), "Invalid value for configuration property");
            CHECK_CONTAINS(object.errorDescription(), "socket.timeout.ms");

            // The same key with a value librdkafka accepts still goes through,
            // so the two failures above are about the arguments and not about
            // SetGlobalConf being broken.
            CHECK_TRUE(object.callBool(u"SetGlobalConf", { "socket.timeout.ms", "12345" }));
            CHECK_EQ(object.errorDescription(), std::string("Sucess"));

            // An empty key and an empty value never reach librdkafka: the
            // component rejects both first. The texts differ per class ("empty
            // key" against "key is empty"), the ERR_BADPARAMETR prefix does not.
            CHECK_FALSE(object.callBool(u"SetGlobalConf", { "", "1" }));
            CHECK_CONTAINS(object.errorDescription(), "Bad parametrs");
            CHECK_FALSE(object.callBool(u"SetGlobalConf", { "client.id", "" }));
            CHECK_CONTAINS(object.errorDescription(), "Bad parametrs");

            // Neither key nor value is a string at all.
            CHECK_FALSE(object.callBool(u"SetGlobalConf", { 1, 2 }));
            CHECK_CONTAINS(object.errorDescription(), "Bad parametrs");

            CHECK_EQ(object.host().errorCount(), std::size_t(0));
            CHECK_EQ(object.memory().liveBlocks(), std::size_t(0));
        }
    });

    //-----------------------------------------------------------------------//
    test::run("security.protocol=SSL with an ssl.ca.location that does not exist", [&] {
        // Both the address and the CA path are fiction, and neither is contacted
        // or stat()ed when the property is set: librdkafka stores the string and
        // only hands it to OpenSSL while the client is being built, which is
        // inside Initialize and still before the first socket. So this is a
        // broker-free case even though it is about TLS.
        const char* const kMissingCa  = "/nonexistent/onec-smoke/no-such-ca.pem";
        const char* const kDeadBroker = "127.0.0.1:65533";

        for (const ClassSpec& spec : kClasses) {
            onec::ComponentObject object(*library, spec.className);
            quiet(object);

            CHECK_TRUE(object.callBool(u"SetGlobalConf", { "security.protocol", "SSL" }));
            CHECK_EQ(object.errorDescription(), std::string("Sucess"));
            CHECK_TRUE(object.callBool(u"SetGlobalConf", { "ssl.ca.location", kMissingCa }));
            CHECK_EQ(object.errorDescription(), std::string("Sucess"));

            const bool        ok    = initializeOk(object, kDeadBroker);
            const std::string error = object.errorDescription();
            test::note(std::string(spec.className) + ": Initialize with a missing CA -> "
                       + (ok ? "true" : "false") + ", ErrorDescription = " + error);

            // All three classes agree. The admin client used to be the odd one
            // out - it returned true / "Sucess" here - and the old comment in
            // this place blamed the C API (rd_kafka_conf_dup + rd_kafka_new)
            // for coming back with a usable handle where RdKafka::*::create()
            // fails. That explanation was WRONG and the asymmetry was not in
            // librdkafka: rd_kafka_new() returned NULL with this exact errstr
            // all along, and KafkaAdminClientCore::Initialize threw the failure
            // away, because `res = GlobalConfDefaultInit(brokers)` had already
            // set res.succes = true and the `if (!rk)` branch set res.error
            // without clearing it. The object was left half-dead: Initialize
            // said true, IsInit() was false, and the next call said
            // "Not initialized". The flag is cleared now, so the admin client
            // rejects exactly what librdkafka rejects, like the other two.
            CHECK_MSG(!ok,
                      std::string(spec.className)
                          + ": Initialize succeeded with a CA file that does not "
                            "exist - ErrorDescription = " + error);
            CHECK_MSG(!error.empty(),
                      std::string(spec.className)
                          + ": Initialize failed with an empty ErrorDescription");
            // librdkafka's own wording. The OpenSSL error code that follows
            // it is version-dependent and is not asserted.
            CHECK_CONTAINS(error, "ssl.ca.location");

            // The half-dead state is gone. This is the sequence that exposed
            // it: the admin client is the only one of the three with a method
            // that reports "Not initialized", and it used to be reachable
            // after an Initialize that had just said true.
            if (std::string(spec.className) == "KafkaAdminClient") {
                object.callFunc(u"GetMetadata", { 1000, onec::Arg::empty() });
                CHECK_CONTAINS(object.errorDescription(), "Not initialized");
            }

            CHECK_EQ(object.host().errorCount(), std::size_t(0));
            CHECK_EQ(object.memory().liveBlocks(), std::size_t(0));
        }
    });

    //-----------------------------------------------------------------------//
    test::run("50 create/Init/Done/destroy cycles per class leave no live block", [&] {
        // The first line of defence against a per-instance leak. The component
        // is going to sit in a 1C process creating and dropping these objects
        // for millions of messages, and every string it hands back comes from
        // the memory manager - so a leak there is a leak in production.
        //
        // The loop body does not assert per iteration: 50 x 3 CHECKs would bury
        // the log and say nothing a counter cannot. Failures are counted and
        // reported once, per class.
        const int kCycles = 50;

        for (const ClassSpec& spec : kClasses) {
            int         cyclesWithLiveBlocks = 0;
            int         cyclesWithWrongName  = 0;
            int         cyclesWithWrongTable = 0;
            std::size_t allocatedBlocks      = 0;
            std::size_t worstLiveBlocks      = 0;

            for (int cycle = 0; cycle < kCycles; ++cycle) {
                // GetClassObject -> Init -> setMemManager -> RegisterExtensionAs.
                onec::ComponentObject object(*library, spec.className);

                // Touch enough of the object that the cycle actually allocates:
                // every method name, property name and ErrorDescription below
                // comes out of ComponentBase::allocString, i.e. out of this
                // object's own MemoryManager.
                if (object.registeredName() != spec.className) {
                    ++cyclesWithWrongName;
                }
                if (object.methodCount() != static_cast<long>(spec.methodCount)
                    || object.propCount() != static_cast<long>(spec.propCount)) {
                    ++cyclesWithWrongTable;
                }
                (void)object.methodNames(0);
                (void)object.methodNames(1);
                (void)object.propNames(0);
                (void)object.propNames(1);
                (void)object.errorDescription();

                const std::size_t live = object.memory().liveBlocks();
                if (live != 0) {
                    ++cyclesWithLiveBlocks;
                    if (live > worstLiveBlocks) {
                        worstLiveBlocks = live;
                    }
                }
                allocatedBlocks += object.memory().totalBlocks();

                // ~ComponentObject: Done() then DestroyObject(). A crash on
                // teardown fails the run; anything the component forgot to hand
                // back would already have been counted above.
            }

            test::note(std::string(spec.className) + ": " + std::to_string(kCycles)
                       + " cycles, " + std::to_string(allocatedBlocks)
                       + " block(s) allocated and released");

            CHECK_MSG(cyclesWithLiveBlocks == 0,
                      std::string(spec.className) + ": " + std::to_string(cyclesWithLiveBlocks)
                          + " of " + std::to_string(kCycles)
                          + " cycles ended with live memory-manager blocks (worst: "
                          + std::to_string(worstLiveBlocks) + ")");
            CHECK_MSG(cyclesWithWrongName == 0,
                      std::string(spec.className) + ": RegisterExtensionAs drifted in "
                          + std::to_string(cyclesWithWrongName) + " cycle(s)");
            CHECK_MSG(cyclesWithWrongTable == 0,
                      std::string(spec.className) + ": the method/property table changed in "
                          + std::to_string(cyclesWithWrongTable) + " cycle(s)");

            // If this is zero the loop above proved nothing - it would mean no
            // string ever crossed the memory manager.
            CHECK_MSG(allocatedBlocks >= static_cast<std::size_t>(kCycles),
                      std::string(spec.className) + ": only " + std::to_string(allocatedBlocks)
                          + " block(s) allocated over " + std::to_string(kCycles)
                          + " cycles - the leak check is not exercising anything");
        }
    });

    //-----------------------------------------------------------------------//
    test::run("repeated create/destroy leaves nothing behind", [&] {
        // A wrong link order or a static initialiser that only survives one pass
        // shows up here, not in the single-object cases above.
        for (int pass = 0; pass < 3; ++pass) {
            for (const ClassSpec& spec : kClasses) {
                onec::ComponentObject object(*library, spec.className);
                CHECK_EQ(object.registeredName(), std::string(spec.className));
                CHECK_EQ(object.methodCount(), static_cast<long>(spec.methodCount));
                CHECK_EQ(object.errorDescription(), std::string("Sucess"));
                CHECK_EQ(object.memory().liveBlocks(), std::size_t(0));
            }
        }
    });

    return test::summary("smoke");
}
