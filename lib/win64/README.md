# lib/win64

Статические библиотеки librdkafka для Windows x64 сюда **не коммитятся**.

Соберите их локально — заголовки в `src/` и эти `.lib` должны быть от одной версии
librdkafka, иначе получите ошибки линковки или, хуже, тихое расхождение ABI:

    powershell -ExecutionPolicy Bypass -File scripts\build-librdkafka-windows.ps1

Скрипт положит сюда `rdkafka.lib`, `rdkafka++.lib` и статические зависимости из vcpkg
(`libssl.lib`, `libcrypto.lib`, `libcurl.lib`, `zlib.lib`, `zstd.lib`).
