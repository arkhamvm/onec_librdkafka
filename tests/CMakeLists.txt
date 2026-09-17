# Standalone CMake project for the onec-librdkafka test suite.
#
# It is deliberately NOT add_subdirectory()'d from the top-level CMakeLists.txt:
# the component and the tests have different lifecycles (the component is built
# once from scripts/build-component-linux.sh, the tests are rebuilt and re-run
# by hand), and the top-level file must stay free of test plumbing.
#
#   cmake -S tests -B tests/build
#   cmake --build tests/build -j
#   ctest --test-dir tests/build --output-on-failure
#
# or simply tests/run-tests.sh, which does all of the above plus the broker.
#
# Options (all cache variables, so -D... on the cmake line):
#
#   ONEC_KAFKA_SO=<path>          the component to load        (see below)
#   ONEC_TESTS_SANITIZE=<mode>    none | address | thread | undefined
#                                 (ON is an accepted synonym for address)
#   ONEC_TESTS_REQUIRE_BROKER=ON  an unreachable broker FAILS instead of
#                                 skipping - use this whenever a green ctest is
#                                 meant to mean "TLS was actually exercised"
#   ONEC_TESTS_SOAK=ON            enable the (opt-in, very slow) soak test
#   ONEC_TESTS_SOAK_TIMEOUT=<s>   its ctest timeout, default 7200
#
# Which test runs when:
#
#   ctest                      smoke + kafka_ssl; soak is reported as Disabled
#   ctest -L soak              only the soak test (needs -DONEC_TESTS_SOAK=ON)
#   ctest -LE soak             everything except the soak test
#
# The component under test is never built here - it is dlopen()ed at runtime.
# A missing .so is therefore a runtime skip/failure with a readable message,
# not a configure error, so `cmake -S tests -B tests/build` works on a clean
# checkout before anything has been compiled.

cmake_minimum_required(VERSION 3.16)
project(onec_librdkafka_tests LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Debug info by default: when a test dies inside librdkafka's threads a usable
# backtrace is worth far more than the last few percent of speed.
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING
        "Build type for the test binaries" FORCE)
endif()

get_filename_component(ONEC_REPO_ROOT "${CMAKE_CURRENT_LIST_DIR}/.." ABSOLUTE)

#----------------------------------------------------------------------------#
# The component under test
#----------------------------------------------------------------------------#

set(ONEC_KAFKA_SO "${ONEC_REPO_ROOT}/out64/librdkafka_onec.so" CACHE FILEPATH
    "Path to the 1C native component (librdkafka_onec.so) the tests load")

# A warning, never an error: librdkafka takes a long time to build and it is
# perfectly reasonable to configure the tests while it is still compiling.
if(NOT EXISTS "${ONEC_KAFKA_SO}")
    message(WARNING
        "The component under test does not exist yet:\n"
        "    ${ONEC_KAFKA_SO}\n"
        "The tests will still build - they dlopen() it at runtime and report a "
        "clean error if it is missing. Build it with "
        "scripts/build-component-linux.sh, or re-configure with "
        "-DONEC_KAFKA_SO=/path/to/librdkafka_onec.so.")
endif()

#----------------------------------------------------------------------------#
# Common compile/link settings
#----------------------------------------------------------------------------#

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# One sanitizer at a time: ASan and TSan cannot be combined, and the soak test
# wants both variants (ASan for leaks, TSan for the component's own threads).
# The historical boolean spelling keeps working - ONEC_TESTS_SANITIZE=ON is
# address, which as before also turns UBSan on alongside it.
set(ONEC_TESTS_SANITIZE "none" CACHE STRING
    "Sanitizer for the test binaries: none|address|thread|undefined (ON == address)")
set_property(CACHE ONEC_TESTS_SANITIZE PROPERTY STRINGS
    none address thread undefined)

string(TOLOWER "${ONEC_TESTS_SANITIZE}" _onec_san_raw)
if(_onec_san_raw MATCHES "^(none|off|false|0|no|n|)$")
    set(ONEC_SANITIZER "none")
elseif(_onec_san_raw MATCHES "^(address|asan|on|true|1|yes|y)$")
    set(ONEC_SANITIZER "address")
elseif(_onec_san_raw MATCHES "^(thread|tsan)$")
    set(ONEC_SANITIZER "thread")
elseif(_onec_san_raw MATCHES "^(undefined|ub|ubsan)$")
    set(ONEC_SANITIZER "undefined")
else()
    message(FATAL_ERROR
        "ONEC_TESTS_SANITIZE='${ONEC_TESTS_SANITIZE}' is not one of "
        "none|address|thread|undefined (ON is accepted as a synonym for address).")
endif()

add_library(onec_test_flags INTERFACE)

target_compile_options(onec_test_flags INTERFACE
    -Wall -Wextra)

if(ONEC_SANITIZER STREQUAL "address")
    # UBSan rides along with ASan exactly as it always has here.
    target_compile_options(onec_test_flags INTERFACE
        -fsanitize=address,undefined -fno-omit-frame-pointer)
    target_link_options(onec_test_flags INTERFACE
        -fsanitize=address,undefined)
elseif(ONEC_SANITIZER STREQUAL "thread")
    target_compile_options(onec_test_flags INTERFACE
        -fsanitize=thread -fno-omit-frame-pointer)
    target_link_options(onec_test_flags INTERFACE
        -fsanitize=thread)
elseif(ONEC_SANITIZER STREQUAL "undefined")
    target_compile_options(onec_test_flags INTERFACE
        -fsanitize=undefined -fno-omit-frame-pointer)
    target_link_options(onec_test_flags INTERFACE
        -fsanitize=undefined)
endif()

#----------------------------------------------------------------------------#
# The host harness: everything the 1C platform would normally provide
#----------------------------------------------------------------------------#

add_library(onec_test_host STATIC
    host/onec_host.cpp
    host/component_loader.cpp)

target_include_directories(onec_test_host PUBLIC
    "${CMAKE_CURRENT_LIST_DIR}")

# The Native API headers are vendored third-party code (AddInDefBase.h:163
# trips -Wignored-qualifiers). SYSTEM keeps their warnings out of our build log
# without weakening -Wall -Wextra on the test code itself.
target_include_directories(onec_test_host SYSTEM PUBLIC
    "${ONEC_REPO_ROOT}/include")

# Baked-in fallback for the .so path. Runtime precedence inside the tests is
# argv[1] > $ONEC_KAFKA_SO > this definition, so an explicit run always wins.
target_compile_definitions(onec_test_host PUBLIC
    ONEC_KAFKA_SO_PATH="${ONEC_KAFKA_SO}")

target_link_libraries(onec_test_host
    PUBLIC  ${CMAKE_DL_LIBS} Threads::Threads
    PRIVATE onec_test_flags)

#----------------------------------------------------------------------------#
# Test binaries - each has its own main(), so three separate executables
#----------------------------------------------------------------------------#

add_executable(smoke_test smoke_test.cpp)
target_link_libraries(smoke_test PRIVATE onec_test_host onec_test_flags)

add_executable(kafka_ssl_test kafka_ssl_test.cpp)
target_link_libraries(kafka_ssl_test PRIVATE onec_test_host onec_test_flags)

# The soak / leak test. Millions of messages and thousands of object lifecycles,
# so it is never part of an ordinary run - see the ctest section below.
if(NOT EXISTS "${CMAKE_CURRENT_LIST_DIR}/soak_test.cpp")
    message(FATAL_ERROR
        "tests/soak_test.cpp is missing.\n"
        "It is part of the suite (target soak_test, ctest label \"soak\"); "
        "restore it from git rather than dropping the target.")
endif()

add_executable(soak_test soak_test.cpp)
target_link_libraries(soak_test PRIVATE onec_test_host onec_test_flags)

#----------------------------------------------------------------------------#
# tools/kafka_null_producer - a helper EXECUTABLE, not a test
#
# KafkaProducer::Produce() always passes a non-NULL payload pointer, so the
# component cannot produce a Kafka tombstone (an ABSENT value) or a header with
# an absent value - the two inputs the JSON builder used to turn into an
# unparseable pool. kafka_ssl_test fork/execs this program to put them in a
# topic.
#
# It is a separate process on purpose. It links librdkafka directly, and the
# test process already has a second, statically linked copy of librdkafka inside
# the dlopen()ed component; keeping them in different processes keeps the two
# from interposing each other's symbols, and keeps librdkafka's own allocations
# out of a LeakSanitizer run of the test binary. For the same reason it is built
# WITHOUT onec_test_flags - only the warning flags, never the sanitizers.
#----------------------------------------------------------------------------#

set(ONEC_RDKAFKA_LIB_DIR "${ONEC_REPO_ROOT}/lib/linux64")

if(EXISTS "${ONEC_RDKAFKA_LIB_DIR}/librdkafka-static.a")
    add_executable(kafka_null_producer tools/kafka_null_producer.cpp)
    target_compile_options(kafka_null_producer PRIVATE -Wall -Wextra)
    # The vendored rdkafka.h lives in src/ next to the component sources.
    target_include_directories(kafka_null_producer SYSTEM PRIVATE "${ONEC_REPO_ROOT}/src")
    target_compile_definitions(kafka_null_producer PRIVATE LIBRDKAFKA_STATICLIB=1)
    target_link_libraries(kafka_null_producer PRIVATE
        "${ONEC_RDKAFKA_LIB_DIR}/librdkafka-static.a"
        Threads::Threads ${CMAKE_DL_LIBS} m)
    # OpenSSL, zlib, zstd, curl and lz4 are archived inside librdkafka-static.a
    # (mklove --source-deps-only), exactly as in the top-level CMakeLists.txt,
    # so nothing else has to be found here. librt is only a separate library on
    # glibc older than 2.34; link it when it exists.
    find_library(ONEC_LIBRT rt)
    if(ONEC_LIBRT)
        target_link_libraries(kafka_null_producer PRIVATE "${ONEC_LIBRT}")
    endif()

    # kafka_ssl_test finds the helper next to its own executable, so the two
    # have to land in the same directory.
    add_dependencies(kafka_ssl_test kafka_null_producer)
    set(ONEC_HAVE_NULL_PRODUCER ON)
else()
    set(ONEC_HAVE_NULL_PRODUCER OFF)
    message(WARNING
        "${ONEC_RDKAFKA_LIB_DIR}/librdkafka-static.a is missing, so the "
        "kafka_null_producer helper will not be built. The tombstone / null "
        "header case in kafka_ssl_test will FAIL rather than quietly pass - "
        "run scripts/build-librdkafka-linux.sh first.")
endif()

#----------------------------------------------------------------------------#
# ctest registration
#----------------------------------------------------------------------------#

enable_testing()

# A skipped test does not colour ctest's exit status, so by default a run with
# no broker exits 0 while nothing at all was proven over TLS. That is the right
# behaviour for "I only want the smoke test right now" and the wrong behaviour
# for anything that is supposed to gate a release, hence the switch.
option(ONEC_TESTS_REQUIRE_BROKER
       "An unreachable broker is a test FAILURE, not a skip" OFF)

# Opt-in, because the soak test runs for minutes to hours.
option(ONEC_TESTS_SOAK
       "Enable the soak test (ctest label \"soak\"); it stays disabled otherwise" OFF)

# 7200 s covers the soak test's own default (a million messages, whose internal
# watchdog is 60 s + 3 ms per message = ~3060 s) with room to spare. Push the
# message count past ~2.3M and this becomes the binding limit, so raise it too.
set(ONEC_TESTS_SOAK_TIMEOUT 7200 CACHE STRING
    "ctest timeout for the soak test, in seconds")

# WORKING_DIRECTORY is the repository root so the relative search paths inside
# the tests (tests/docker/secrets/ca.pem in particular) resolve the same way
# they do for a hand-run binary. The .so is passed as argv[1] so the configured
# ONEC_KAFKA_SO is authoritative and no path guessing happens under ctest.

add_test(NAME smoke
         COMMAND smoke_test "${ONEC_KAFKA_SO}"
         WORKING_DIRECTORY "${ONEC_REPO_ROOT}")
set_tests_properties(smoke PROPERTIES
    TIMEOUT 120)

add_test(NAME kafka_ssl
         COMMAND kafka_ssl_test "${ONEC_KAFKA_SO}"
         WORKING_DIRECTORY "${ONEC_REPO_ROOT}")

# The test has its own 300 s watchdog (KAFKA_TEST_HARD_TIMEOUT_MS) and exits 3
# when it fires; TIMEOUT is only the backstop behind it.
set_tests_properties(kafka_ssl PROPERTIES TIMEOUT 420)

if(ONEC_TESTS_REQUIRE_BROKER)
    # No SKIP_RETURN_CODE: exit 77 stays a plain failure. KAFKA_TEST_REQUIRE_BROKER
    # additionally makes the test itself refuse to skip, so the failure carries a
    # description of what could not be reached instead of a bare exit code.
    set_tests_properties(kafka_ssl PROPERTIES
        ENVIRONMENT "KAFKA_TEST_REQUIRE_BROKER=1")
else()
    # 77 == "the broker is not reachable / the CA is missing". Reported by
    # ctest as "Skipped", never as a pass: nothing was tested.
    #
    # Careful: a skip does NOT fail ctest, so `ctest` exiting 0 here only means
    # "nothing broke", not "TLS works". Configure with
    # -DONEC_TESTS_REQUIRE_BROKER=ON when the exit status has to mean the latter.
    set_tests_properties(kafka_ssl PROPERTIES SKIP_RETURN_CODE 77)
endif()

#----------------------------------------------------------------------------#
# The soak test: registered always, run only on request.
#
#   ctest                        smoke + kafka_ssl; soak is reported "Disabled"
#   cmake -B build -DONEC_TESTS_SOAK=ON && ctest -L soak
#                                only the soak test
#   ctest -LE soak               everything except the soak test
#
# A label alone would not keep it out of a plain `ctest` run (labels select, they
# do not exclude), so the DISABLED property does that part and the label is what
# you select it with once it is enabled.
#----------------------------------------------------------------------------#

add_test(NAME soak
         COMMAND soak_test "${ONEC_KAFKA_SO}"
         WORKING_DIRECTORY "${ONEC_REPO_ROOT}")
set_tests_properties(soak PROPERTIES
    LABELS "soak"
    TIMEOUT ${ONEC_TESTS_SOAK_TIMEOUT})

if(NOT ONEC_TESTS_SOAK)
    set_tests_properties(soak PROPERTIES DISABLED TRUE)
endif()

if(ONEC_TESTS_REQUIRE_BROKER)
    set_tests_properties(soak PROPERTIES
        ENVIRONMENT "KAFKA_TEST_REQUIRE_BROKER=1")
else()
    set_tests_properties(soak PROPERTIES SKIP_RETURN_CODE 77)
endif()

#----------------------------------------------------------------------------#

message(STATUS "onec-librdkafka tests")
message(STATUS "  build type     : ${CMAKE_BUILD_TYPE}")
message(STATUS "  component      : ${ONEC_KAFKA_SO}")
message(STATUS "  sanitizer      : ${ONEC_SANITIZER}")
if(ONEC_HAVE_NULL_PRODUCER)
    message(STATUS "  null producer  : built (tombstone / null-header case is live)")
else()
    message(STATUS "  null producer  : NOT built - lib/linux64/librdkafka-static.a is missing")
endif()
if(ONEC_TESTS_REQUIRE_BROKER)
    message(STATUS "  broker         : REQUIRED - an unreachable broker fails the run")
else()
    message(STATUS "  broker         : optional - kafka_ssl skips (exit 77) when it is down,")
    message(STATUS "                   and a skip does not fail ctest. Use "
                   "-DONEC_TESTS_REQUIRE_BROKER=ON to change that.")
endif()
if(ONEC_TESTS_SOAK)
    message(STATUS "  soak test      : enabled, select it with 'ctest -L soak' "
                   "(timeout ${ONEC_TESTS_SOAK_TIMEOUT}s)")
else()
    message(STATUS "  soak test      : disabled - configure with -DONEC_TESTS_SOAK=ON, "
                   "then 'ctest -L soak'")
endif()
message(STATUS "  run with       : ctest --test-dir ${CMAKE_CURRENT_BINARY_DIR} --output-on-failure")
