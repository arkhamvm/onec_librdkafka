#ifndef ONEC_TESTS_HOST_TEST_ASSERT_H
#define ONEC_TESTS_HOST_TEST_ASSERT_H

// A test harness small enough to read in one sitting. No framework: nothing is
// installed on the machines these tests have to run on.
//
//   int main() {
//       test::run("consumer talks TLS to Kafka 4.x", [] {
//           onec::ComponentLibrary lib(onec::ComponentLibrary::defaultPath());
//           onec::ComponentObject  consumer(lib, u"KafkaConsumer");
//           REQUIRE(consumer.callBool(u"Initialize", {broker, "test-group"}));
//           CHECK_EQ(consumer.errorDescription(), std::string());
//       });
//       return test::summary("ssl_smoke");
//   }
//
// CHECK*  records a failure and carries on.
// REQUIRE records a failure and abandons the current test::run body.

#include <exception>
#include <functional>
#include <iostream>
#include <sstream>
#include <string>

namespace test {

//---------------------------------------------------------------------------//
// Counters
//---------------------------------------------------------------------------//

inline int& checkCount()
{
    static int value = 0;
    return value;
}

inline int& failCount()
{
    static int value = 0;
    return value;
}

inline std::string& currentCase()
{
    static std::string value;
    return value;
}

// Thrown by REQUIRE; test::run swallows it, the failure is already recorded.
class RequirementFailed : public std::exception
{
public:
    explicit RequirementFailed(std::string what) : what_(std::move(what)) {}
    const char* what() const noexcept override { return what_.c_str(); }
private:
    std::string what_;
};

//---------------------------------------------------------------------------//
// Value printing
//---------------------------------------------------------------------------//

template <typename T>
std::string toStr(const T& value)
{
    std::ostringstream os;
    os << value;
    return os.str();
}

inline std::string toStr(bool value) { return value ? "true" : "false"; }
inline std::string toStr(const std::string& value) { return "\"" + value + "\""; }
inline std::string toStr(const char* value) { return value != nullptr ? "\"" + std::string(value) + "\"" : "(null)"; }
inline std::string toStr(std::nullptr_t) { return "nullptr"; }

//---------------------------------------------------------------------------//
// Reporting
//---------------------------------------------------------------------------//

inline void reportPass()
{
    ++checkCount();
}

inline void reportFail(const char* file, int line, const std::string& message)
{
    ++checkCount();
    ++failCount();
    std::cout << "  FAIL  " << file << ":" << line << "  " << message << std::endl;
}

inline std::string failText(const char* expression, const std::string& detail)
{
    std::string text = expression;
    if (!detail.empty()) {
        text += "  ->  ";
        text += detail;
    }
    return text;
}

// An informational line, for context that is not an assertion.
inline void note(const std::string& message)
{
    std::cout << "  ...   " << message << std::endl;
}

//---------------------------------------------------------------------------//
// Driver
//---------------------------------------------------------------------------//

inline void run(const std::string& name, const std::function<void()>& body)
{
    currentCase() = name;
    const int before = failCount();
    std::cout << "[ RUN  ] " << name << std::endl;

    try {
        body();
    } catch (const RequirementFailed&) {
        // Already reported by REQUIRE.
    } catch (const std::exception& e) {
        reportFail("<test::run>", 0, name + ": unexpected exception: " + e.what());
    } catch (...) {
        reportFail("<test::run>", 0, name + ": unexpected non-standard exception");
    }

    const bool ok = failCount() == before;
    std::cout << (ok ? "[  OK  ] " : "[ FAIL ] ") << name << std::endl;
    currentCase().clear();
}

// Call once from main() and return its result.
inline int summary(const std::string& suite = std::string())
{
    std::cout << "-------------------------------------------------------------" << std::endl;
    std::cout << (suite.empty() ? "tests" : suite) << ": "
              << checkCount() << " check(s), "
              << failCount() << " failure(s)" << std::endl;
    return failCount() == 0 ? 0 : 1;
}

} // namespace test

//---------------------------------------------------------------------------//
// Macros
//---------------------------------------------------------------------------//

#define CHECK(cond)                                                            \
    do {                                                                       \
        if (cond) {                                                            \
            ::test::reportPass();                                              \
        } else {                                                               \
            ::test::reportFail(__FILE__, __LINE__,                             \
                ::test::failText("CHECK(" #cond ")", std::string()));          \
        }                                                                      \
    } while (0)

#define CHECK_MSG(cond, message)                                               \
    do {                                                                       \
        if (cond) {                                                            \
            ::test::reportPass();                                              \
        } else {                                                               \
            ::test::reportFail(__FILE__, __LINE__,                             \
                ::test::failText("CHECK(" #cond ")", std::string(message)));   \
        }                                                                      \
    } while (0)

#define CHECK_TRUE(cond)  CHECK(cond)
#define CHECK_FALSE(cond) CHECK(!(cond))

// The operands are copied, not bound by reference: CHECK_EQ(f()[0].field, x)
// would otherwise read a subobject of a temporary that is already gone.
#define CHECK_EQ(actual, expected)                                             \
    do {                                                                       \
        auto onec_a_ = (actual);                                               \
        auto onec_e_ = (expected);                                             \
        if (onec_a_ == onec_e_) {                                              \
            ::test::reportPass();                                              \
        } else {                                                               \
            ::test::reportFail(__FILE__, __LINE__,                             \
                ::test::failText("CHECK_EQ(" #actual ", " #expected ")",       \
                    "actual " + ::test::toStr(onec_a_)                         \
                    + ", expected " + ::test::toStr(onec_e_)));                \
        }                                                                      \
    } while (0)

#define CHECK_NE(actual, unexpected)                                           \
    do {                                                                       \
        auto onec_a_ = (actual);                                               \
        auto onec_u_ = (unexpected);                                           \
        if (!(onec_a_ == onec_u_)) {                                           \
            ::test::reportPass();                                              \
        } else {                                                               \
            ::test::reportFail(__FILE__, __LINE__,                             \
                ::test::failText("CHECK_NE(" #actual ", " #unexpected ")",     \
                    "both are " + ::test::toStr(onec_a_)));                    \
        }                                                                      \
    } while (0)

// Substring match - the natural shape of an assertion about an error text.
#define CHECK_CONTAINS(haystack, needle)                                       \
    do {                                                                       \
        const std::string onec_h_ = (haystack);                                \
        const std::string onec_n_ = (needle);                                  \
        if (onec_h_.find(onec_n_) != std::string::npos) {                      \
            ::test::reportPass();                                              \
        } else {                                                               \
            ::test::reportFail(__FILE__, __LINE__,                             \
                ::test::failText("CHECK_CONTAINS(" #haystack ", " #needle ")", \
                    ::test::toStr(onec_h_) + " does not contain "              \
                    + ::test::toStr(onec_n_)));                                \
        }                                                                      \
    } while (0)

#define CHECK_THROWS(expr)                                                     \
    do {                                                                       \
        bool onec_threw_ = false;                                              \
        try { (void)(expr); } catch (...) { onec_threw_ = true; }              \
        if (onec_threw_) {                                                     \
            ::test::reportPass();                                              \
        } else {                                                               \
            ::test::reportFail(__FILE__, __LINE__,                             \
                ::test::failText("CHECK_THROWS(" #expr ")", "nothing thrown"));\
        }                                                                      \
    } while (0)

#define CHECK_THROWS_AS(expr, exception_type)                                  \
    do {                                                                       \
        int onec_state_ = 0;                                                   \
        try { (void)(expr); }                                                  \
        catch (const exception_type&) { onec_state_ = 1; }                     \
        catch (...) { onec_state_ = 2; }                                       \
        if (onec_state_ == 1) {                                                \
            ::test::reportPass();                                              \
        } else {                                                               \
            ::test::reportFail(__FILE__, __LINE__,                             \
                ::test::failText("CHECK_THROWS_AS(" #expr ", " #exception_type ")", \
                    onec_state_ == 0 ? "nothing thrown" : "a different type was thrown")); \
        }                                                                      \
    } while (0)

#define CHECK_NO_THROW(expr)                                                   \
    do {                                                                       \
        try {                                                                  \
            (void)(expr);                                                      \
            ::test::reportPass();                                              \
        } catch (const std::exception& onec_ex_) {                             \
            ::test::reportFail(__FILE__, __LINE__,                             \
                ::test::failText("CHECK_NO_THROW(" #expr ")",                  \
                    std::string("threw: ") + onec_ex_.what()));                \
        } catch (...) {                                                        \
            ::test::reportFail(__FILE__, __LINE__,                             \
                ::test::failText("CHECK_NO_THROW(" #expr ")",                  \
                    "threw a non-standard exception"));                        \
        }                                                                      \
    } while (0)

// Aborts the enclosing test::run body.
#define REQUIRE(cond)                                                          \
    do {                                                                       \
        if (cond) {                                                            \
            ::test::reportPass();                                              \
        } else {                                                               \
            ::test::reportFail(__FILE__, __LINE__,                             \
                ::test::failText("REQUIRE(" #cond ")", std::string()));        \
            throw ::test::RequirementFailed(                                   \
                std::string("REQUIRE(" #cond ") failed at ")                   \
                + __FILE__ + ":" + std::to_string(__LINE__));                  \
        }                                                                      \
    } while (0)

// The message is evaluated exactly once, into a local. Call sites read like
// REQUIRE_MSG(x, "... failed: " + consumer.errorDescription()) or
// REQUIRE_MSG(h, std::string("dlopen: ") + ::dlerror()); expanding that twice
// would mean two more round trips into the component for every failure - and
// for anything that is not idempotent, two different texts in the report and in
// the exception.
#define REQUIRE_MSG(cond, message)                                             \
    do {                                                                       \
        if (cond) {                                                            \
            ::test::reportPass();                                              \
        } else {                                                               \
            const std::string onec_msg_((message));                            \
            ::test::reportFail(__FILE__, __LINE__,                             \
                ::test::failText("REQUIRE(" #cond ")", onec_msg_));            \
            throw ::test::RequirementFailed(                                   \
                std::string("REQUIRE(" #cond ") failed at ")                   \
                + __FILE__ + ":" + std::to_string(__LINE__)                    \
                + ": " + onec_msg_);                                           \
        }                                                                      \
    } while (0)

#define FAIL(message)                                                          \
    ::test::reportFail(__FILE__, __LINE__, std::string(message))

#endif // ONEC_TESTS_HOST_TEST_ASSERT_H
