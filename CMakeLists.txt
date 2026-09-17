cmake_minimum_required(VERSION 3.10)
project(rdkafka_onec)

set(CMAKE_CXX_STANDARD 17)
set(TARGET rdkafka_onec)

if( CMAKE_SIZEOF_VOID_P EQUAL 8 )
	set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/../out64)
	set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/../out64)
	set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/../out64)
else()
	set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/../out32)
	set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/../out32)
	set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/../out32)
endif()

list(APPEND SOURCES
    src/exports.def
    src/dllmain.cpp
    src/stdafx.cpp
    src/stdafx.h
    src/export.h
    src/export.cpp
    src/rdkafka.h
    src/rdkafkacpp.h
    src/base64.h
    src/base64.cpp
    src/gettime.h
    src/gettime.cpp
    src/nlohmann/json.hpp
    src/component_types.h
    src/ComponentBaseImp.h
    src/ComponentBaseImp.cpp
    src/data_conversion.h
    src/data_builder.cpp
    src/data_conteiner.cpp
    src/errors.cpp
    src/consumer1c.h
    src/consumer1c.cpp
    src/consumer1c_rebalance_cb.cpp
    src/consumer1c_event_cb.cpp
    src/consumer1c_core.cpp
    src/producer1c.h
    src/producer1c.cpp
    src/producer1c_delivery_report.cpp
    src/producer1c_core.cpp
    src/admin_client1c.h
    src/admin_client1c.cpp
    src/admin_client1c_core.cpp
    )

add_library(${TARGET} SHARED ${SOURCES})
target_include_directories(${TARGET} PRIVATE include)

# librdkafka is linked statically on every platform.
# Upstream rdkafka.h / rdkafkacpp.h are kept pristine (see scripts/update-librdkafka-headers.sh),
# so the static-link switch has to be set here instead of being patched into the headers.
target_compile_definitions(${TARGET} PRIVATE LIBRDKAFKA_STATICLIB=1)

#----------------------------------------------------------------------------------------------------------------#
if (WIN32 AND NOT MSVC)
    message(FATAL_ERROR "Must be compiled with MSVC on Windows")
endif ()

if (UNIX)
    find_package( Threads )
else()
    # WIN32_LEAN_AND_MEAN keeps <windows.h> from pulling in winsock v1, which clashes
    # with the <winsock2.h> that rdkafka.h includes. src/stdafx.h pins the include order.
    add_definitions(-DUNICODE -DWIN32 -DWIN32_LEAN_AND_MEAN -D_USRDLL -D_CRT_SECURE_NO_WARNINGS -D_CRT_SECURE_NO_DEPRECATE -DADDINCPP_EXPORTS -D_SILENCE_CXX17_CODECVT_HEADER_DEPRECATION_WARNING)
endif()

if (UNIX)
    if (${CMAKE_SIZEOF_VOID_P} MATCHES "8")
        set(CMAKE_C_FLAGS "-m64 ${CMAKE_C_FLAGS}")
        set(CMAKE_CXX_FLAGS "-m64 ${CMAKE_CXX_FLAGS}")
        set(RDKAFKA_LIB_DIR ${CMAKE_SOURCE_DIR}/lib/linux64)
    else()
        message(FATAL_ERROR
            "32-bit Linux is not supported by this fork: lib/linux32 holds no librdkafka 2.15.1 build. "
            "Run scripts/build-librdkafka-linux.sh with a 32-bit toolchain (gcc-multilib) to produce one.")
    endif()

    foreach(_lib librdkafka++.a librdkafka-static.a)
        if (NOT EXISTS ${RDKAFKA_LIB_DIR}/${_lib})
            message(FATAL_ERROR "${RDKAFKA_LIB_DIR}/${_lib} is missing. Run scripts/build-librdkafka-linux.sh first.")
        endif()
    endforeach()

    # librdkafka++.a must precede librdkafka-static.a: the C++ wrapper depends on the C core.
    target_link_libraries(${TARGET} ${RDKAFKA_LIB_DIR}/librdkafka++.a)
    target_link_libraries(${TARGET} ${RDKAFKA_LIB_DIR}/librdkafka-static.a)
    # OpenSSL, zlib, zstd, curl and lz4 are already archived inside librdkafka-static.a
    # (mklove --source-deps-only), so only the system libraries are left to link.
    target_link_libraries(${TARGET} ${CMAKE_THREAD_LIBS_INIT})
    target_link_libraries(${TARGET} ${CMAKE_DL_LIBS})
    target_link_libraries(${TARGET} m)
    target_link_libraries(${TARGET} rt)
endif()

if (WIN32)
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /MT")
    target_precompile_headers(${TARGET} PUBLIC src/stdafx.h)

    if (${CMAKE_SIZEOF_VOID_P} MATCHES "8")
        set(RDKAFKA_LIB_DIR ${CMAKE_SOURCE_DIR}/lib/win64)
    else()
        set(RDKAFKA_LIB_DIR ${CMAKE_SOURCE_DIR}/lib/win32)
    endif()

    # scripts/build-librdkafka-windows.ps1 writes exactly two groups of archives here:
    # rdkafka.lib / rdkafka++.lib from the librdkafka build, and the static vcpkg
    # dependencies copied next to them. lz4 is not among them - the script builds with
    # ENABLE_LZ4_EXT=OFF, so the bundled lz4 is archived inside rdkafka.lib.
    #
    # The names are spelled out instead of globbed on purpose. lib/win64 and lib/win32 are
    # tracked directories whose .lib files git ignores, and the script only wipes them
    # when it actually runs, so an archive from a different librdkafka version can end up
    # sitting here unnoticed. A glob would link it silently next to the current one: the C
    # symbol names match, the struct layouts do not, and the result is memory corruption at
    # run time rather than a link error (see lib/win64/README.md). The same goes for a vcpkg
    # tree that emits both CRT flavours - zlib.lib next to zlibd.lib would both be picked up.
    #
    # rdkafka++.lib is listed before rdkafka.lib for the same reason as on Linux: the C++
    # wrapper depends on the C core.
    set(RDKAFKA_WIN_REQUIRED_LIBS rdkafka++.lib rdkafka.lib)
    # The build script refuses to finish when one of these is missing from the vcpkg tree.
    # They stay optional here so that RDKAFKA_WIN_EXTRA_LIBS can stand in for one that vcpkg
    # renamed; a genuinely missing one still surfaces as an unresolved symbol.
    set(RDKAFKA_WIN_OPTIONAL_LIBS libssl.lib libcrypto.lib libcurl.lib zlib.lib zstd.lib)
    set(RDKAFKA_WIN_EXTRA_LIBS "" CACHE STRING
        "Extra .lib file names in lib/win64 / lib/win32 to accept and link, for the case where vcpkg renames a dependency. Names only, no paths: -DRDKAFKA_WIN_EXTRA_LIBS=ssl.lib;crypto.lib")

    # CONFIGURE_DEPENDS re-runs the glob at build time. Without it the directory is only
    # inspected when CMake configures, and the documented Windows order (configure, then
    # (re)build librdkafka, then build the component) would hide both a .lib that appeared
    # afterwards and a stale one that was added afterwards until someone re-ran CMake by hand.
    if (CMAKE_VERSION VERSION_LESS 3.12)
        file(GLOB RDKAFKA_WIN_FOUND_LIBS ${RDKAFKA_LIB_DIR}/*.lib)
    else()
        file(GLOB RDKAFKA_WIN_FOUND_LIBS CONFIGURE_DEPENDS ${RDKAFKA_LIB_DIR}/*.lib)
    endif()
    if (NOT RDKAFKA_WIN_FOUND_LIBS)
        message(FATAL_ERROR
            "No .lib files in ${RDKAFKA_LIB_DIR}. Run scripts/build-librdkafka-windows.ps1 first.")
    endif()

    # Anything else in the directory is a leftover. Fail loudly instead of linking it.
    set(RDKAFKA_WIN_ALLOWED_LIBS ${RDKAFKA_WIN_REQUIRED_LIBS} ${RDKAFKA_WIN_OPTIONAL_LIBS})
    foreach(_lib IN LISTS RDKAFKA_WIN_EXTRA_LIBS)
        string(TOLOWER "${_lib}" _lib_lower)
        list(APPEND RDKAFKA_WIN_ALLOWED_LIBS ${_lib_lower})
    endforeach()

    set(RDKAFKA_WIN_UNEXPECTED_LIBS "")
    foreach(_lib_path IN LISTS RDKAFKA_WIN_FOUND_LIBS)
        get_filename_component(_lib_name "${_lib_path}" NAME)
        # NTFS is case-insensitive, so the on-disk spelling is not a reliable key.
        string(TOLOWER "${_lib_name}" _lib_lower)
        if (NOT _lib_lower IN_LIST RDKAFKA_WIN_ALLOWED_LIBS)
            list(APPEND RDKAFKA_WIN_UNEXPECTED_LIBS ${_lib_name})
        endif()
    endforeach()

    if (RDKAFKA_WIN_UNEXPECTED_LIBS)
        string(REPLACE ";" ", " _unexpected "${RDKAFKA_WIN_UNEXPECTED_LIBS}")
        string(REPLACE ";" ", " _expected
            "${RDKAFKA_WIN_REQUIRED_LIBS};${RDKAFKA_WIN_OPTIONAL_LIBS}")
        message(FATAL_ERROR
            "Unexpected .lib file(s) in ${RDKAFKA_LIB_DIR}: ${_unexpected}. "
            "Only what scripts/build-librdkafka-windows.ps1 puts there belongs there: ${_expected}. "
            "A leftover from another librdkafka version, or a debug-CRT twin such as zlibd.lib, "
            "would be linked in alongside the right archive and corrupt memory at run time. "
            "Delete the file(s) listed above and re-run scripts/build-librdkafka-windows.ps1. "
            "If vcpkg really did rename a dependency, accept the new name explicitly with "
            "-DRDKAFKA_WIN_EXTRA_LIBS=<name>.lib instead of putting the glob back.")
    endif()

    set(RDKAFKA_WIN_LIBS "")
    foreach(_lib IN LISTS RDKAFKA_WIN_REQUIRED_LIBS)
        if (NOT EXISTS ${RDKAFKA_LIB_DIR}/${_lib})
            message(FATAL_ERROR
                "${RDKAFKA_LIB_DIR}/${_lib} is missing. Run scripts/build-librdkafka-windows.ps1 first.")
        endif()
        list(APPEND RDKAFKA_WIN_LIBS ${RDKAFKA_LIB_DIR}/${_lib})
    endforeach()

    foreach(_lib IN LISTS RDKAFKA_WIN_OPTIONAL_LIBS RDKAFKA_WIN_EXTRA_LIBS)
        if (EXISTS ${RDKAFKA_LIB_DIR}/${_lib})
            list(APPEND RDKAFKA_WIN_LIBS ${RDKAFKA_LIB_DIR}/${_lib})
        else()
            # The build script does not leave a gap like this, so lib\win64 was filled by hand.
            message(WARNING
                "${RDKAFKA_LIB_DIR}/${_lib} not found. If the component needs it, the link step "
                "will fail on unresolved OpenSSL / curl / zlib / zstd symbols.")
        endif()
    endforeach()

    target_link_libraries(${TARGET} ${RDKAFKA_WIN_LIBS})

    # System libraries pulled in by a static OpenSSL + curl + librdkafka build.
    # iphlpapi is curl's (if_nametoindex and friends): vcpkg's libcurl.pc lists it.
    target_link_libraries(${TARGET}
        ws2_32.lib
        crypt32.lib
        secur32.lib
        bcrypt.lib
        wldap32.lib
        normaliz.lib
        advapi32.lib
        user32.lib
        shell32.lib
        ole32.lib
        iphlpapi.lib)
endif()

set_target_properties( ${TARGET} PROPERTIES
       POSITION_INDEPENDENT_CODE ON
       CXX_STANDARD 17
       CXX_STANDARD_REQUIRED ON
   )
