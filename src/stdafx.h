// stdafx.h : include file for standard system include files,
// or project specific include files that are used frequently, but
// are changed infrequently
//
#ifndef __STDAFX_H__
#define __STDAFX_H__

#ifdef _WINDOWS
    // <winsock2.h> must come before <windows.h>: the 1C headers pull in <windows.h>,
    // which would otherwise drag in the winsock v1 declarations that rdkafka.h's
    // <winsock2.h> then redefines. This header is the PCH, so it fixes the order globally.
    #ifndef WIN32_LEAN_AND_MEAN
        #define WIN32_LEAN_AND_MEAN
    #endif
    #include <winsock2.h>
    #include <ws2tcpip.h>
    #include <windows.h>
#endif //_WINDOWS

#if defined(__linux__) || defined(__APPLE__)
	#define LINUX_OR_MACOS
#endif

#endif //__STDAFX_H__
