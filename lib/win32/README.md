# lib/win32

Статические библиотеки librdkafka для Windows x86 (32 бита) сюда **не коммитятся**.

Соберите их локально — заголовки в `src/` и эти `.lib` должны быть от одной версии
librdkafka, иначе получите ошибки линковки или, хуже, тихое расхождение ABI:

    powershell -ExecutionPolicy Bypass -File scripts\build-librdkafka-windows.ps1 -Arch x86

Скрипт положит сюда `rdkafka.lib`, `rdkafka++.lib` и статические зависимости из vcpkg
(`libssl.lib`, `libcrypto.lib`, `libcurl.lib`, `zlib.lib`, `zstd.lib`).
