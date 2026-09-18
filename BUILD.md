# Сборка onec-librdkafka

Подробное руководство: от чистой машины до бинарника, который подключается в 1С.
Краткая выжимка есть в [README.md](README.md) — этот файл её не заменяет, а разворачивает.

**Что здесь проверено.** Linux: Ubuntu 24.04, g++ 13.3.0, cmake 3.28.3, perl 5.38.2,
git 2.43.0, docker 27.1.1 + compose v2.29.1. Windows: Windows 11 Pro (сборка 26200),
Visual Studio Community 2022 17.9 (MSVC 14.39, Windows SDK 10.0.19041), cmake 3.28 из
поставки VS, Windows PowerShell 5.1, Git 2.45.1.

| | |
|---|---|
| Выполнено дословно | сборка компоненты через CMake, проверки `nm` / `ldd` / `strings` / `stat -f` / `findmnt` из [§1](#1-что-собирается), [§6](#6-как-убедиться-что-сборка-живая) и [§8](#8-диагностика), смоук-тест, `tests/run-tests.sh --no-docker` (выход 3), конфигурация проекта тестов со всеми ручками из [§9](#9-ручки-сборки), воспроизведение отказа `install(1)` на fuseblk из [§8.1](#81-install-operation-not-permitted-при-сборке-зависимостей), упаковка макета из [§7.1](#71-zip-макет-внешней-компоненты) — прогон на Linux-`.so` и Windows-DLL x64 вместе, плюс отказы при неверной разрядности, постороннем бинарнике, недопустимом `--name` и отсутствующем `unzip`. Сборочные каталоги при этом уводились во временные, чтобы не переписывать `out64/` и `tests/build/` |
| Не выполнялось целиком | ступень 1 (пересборка librdkafka: 10–20 минут, переписывает `lib/linux64` и `src/*.h`) и полный `tests/run-tests.sh` с поднятием брокера — их поведение разобрано по коду скриптов и mklove, с цитатами точных сообщений об ошибках |
| Выполнено на Windows x64 | обе ступени из [§4](#4-windows-x64--x86) с нуля: vcpkg клонирован впервые, зависимости собраны им из исходников при пустом кэше, затем librdkafka и компонента. Плюс проверки готового DLL из [§6.6](#66-проверки-готового-dll-на-windows); смоук-тест — через временный порт `tests/` вне репозитория, см. там же |
| Не выполнялось вовсе | Windows x86 ([§5](#5-32-бита)); TLS-тест против брокера и soak на Windows |

---

## 1. Что собирается

### 1.1 Артефакты

| Платформа | Файл | Чем собирается |
|---|---|---|
| Linux x86_64 | `out64/librdkafka_onec.so` | g++ + CMake, `scripts/build-component-linux.sh` |
| Windows x64 | `out64\Release\rdkafka_onec.dll` | MSVC + CMake, `scripts\build-component-windows.ps1` |
| Windows x86 | `out32\Release\rdkafka_onec.dll` | то же, с `-Arch x86` |

Про подкаталог `Release\` на Windows — см. [§4.6](#46-где-именно-оказывается-dll).
Linux x86 (32 бита) не поддерживается — см. [§5](#5-32-бита).

Из этих файлов собирается ZIP-макет внешней компоненты — [§7.1](#71-zip-макет-внешней-компоненты).

### 1.2 Сборка двухступенчатая

| Ступень | Скрипт | Как часто нужна | Что кладёт |
|---|---|---|---|
| 1. librdkafka | `scripts/build-librdkafka-linux.sh`<br>`scripts\build-librdkafka-windows.ps1` | только при смене версии librdkafka | `lib/linux64/*.a` или `lib/win64/*.lib`, плюс `src/rdkafka.h` и `src/rdkafkacpp.h` |
| 2. компонента | `scripts/build-component-linux.sh`<br>`scripts\build-component-windows.ps1` | при каждой правке `src/` | `out64/…` |

Для **Linux ступень 1 уже сделана и закоммичена**:

```bash
du -h lib/linux64/*.a
```

```
6,5M	lib/linux64/librdkafka++.a
22M	lib/linux64/librdkafka-static.a
```

То есть обычный рабочий цикл на Linux — это только ступень 2, десятки секунд.
Ступень 1 нужна, когда меняется версия librdkafka.

Для **Windows ступень 1 обязательна всегда**: `.lib` в репозиторий не коммитятся
(почему — [§4.4](#44-почему-lib-не-лежат-в-репозитории)).

Третьей ступени нет: `scripts/package-addin.sh` ([§7.1](#71-zip-макет-внешней-компоненты))
ничего не компилирует, а складывает уже собранные файлы в ZIP.

### 1.3 Что влинковано внутрь

librdkafka линкуется статически на обеих платформах. Переключатель
`LIBRDKAFKA_STATICLIB=1` задан не в заголовках (они вендорятся без правок), а в
`CMakeLists.txt` через `target_compile_definitions`.

| Зависимость | Версия | Linux | Windows |
|---|---|---|---|
| librdkafka | 2.15.1 | `lib/linux64/librdkafka-static.a` + `librdkafka++.a` | `lib/win64/rdkafka.lib` + `rdkafka++.lib` |
| OpenSSL (libssl + libcrypto) | 3.5.7 | собран mklove из исходников и **заархивирован внутрь** `librdkafka-static.a` | отдельные `libssl.lib` / `libcrypto.lib` из vcpkg |
| zlib | 1.3.2 | там же, внутри | `zlib.lib` из vcpkg (в vcpkg файл называется `zs.lib`, скрипт копирует его под именем `zlib.lib`) |
| zstd | 1.5.7 | там же, внутри | `zstd.lib` из vcpkg |
| curl | 8.21.0 | там же, внутри | `libcurl.lib` из vcpkg, TLS через Schannel |
| lz4, snappy, cJSON, crc32c… | вендорятся внутри librdkafka | внутри | внутри |

Версии зависимостей — это версии, зашитые в рецепты mklove той самой librdkafka
2.15.1 (`mklove/modules/configure.libssl`, `.zlib`, `.libzstd`, `.libcurl`), а не
что-то, что можно выбрать флагом.

Колонка «Версия» — про Linux. На Windows версии задаёт дерево портов vcpkg, которое
оказалось в `.build\vcpkg` при клоне, и со временем они уходят вперёд. На проверенной
сборке это были OpenSSL 3.6.4, curl 8.22.0, zlib 1.3.2, zstd 1.5.7.

curl там собран с TLS через Schannel (vcpkg разворачивает `curl[core,ssl]` в
`curl[core,ssl,sspi]`), а не через OpenSSL. Соединения с брокерами librdkafka на обеих
платформах шифрует через OpenSSL, но HTTPS-запросы OIDC к
`sasl.oauthbearer.token.endpoint.url` она делает через curl, то есть на Windows — через
Schannel, с проверкой сертификата по хранилищу Windows. Файл или PEM из
`https.ca.location` / `https.ca.pem` учитываются, а OpenSSL-ветки librdkafka для этих
запросов — `https.ca.location=probe`, каталог с сертификатами,
`ssl.ca.certificate.stores` — на Windows не действуют: Schannel не поддерживает ни
`CURLOPT_SSL_CTX_FUNCTION`, ни `CURLOPT_CAPATH`.

Проверить состав можно прямо в готовом бинарнике:

```bash
strings -a out64/librdkafka_onec.so | grep -m1 STATIC_LINKING
```

```
STATIC_LINKING GCC GXX PKGCONFIG INSTALL GNULD LDS C11THREADS LIBDL PLUGINS ZLIB SSL
ZSTD CURL HDRHISTOGRAM SYSLOG SNAPPY SOCKEM SASL_SCRAM SASL_OAUTHBEARER
OAUTHBEARER_OIDC CRC32C_HW
```

`SSL`, `ZSTD`, `CURL`, `ZLIB` в строке есть. `SASL_CYRUS` — нет: GSSAPI/Kerberos
выключен намеренно, см. [§8.2](#82-failed-to-install-dependency-libsasl2).

На Windows librdkafka берёт эту строку не из CMake, а из `src\win32_config.h`, поэтому
она короче: `SSL ZLIB SNAPPY ZSTD CURL SASL_SCRAM SASL_OAUTHBEARER PLUGINS HDRHISTOGRAM`.
Как найти её в DLL — [§6.6](#66-проверки-готового-dll-на-windows).

### 1.4 Рантайм-зависимости Linux-сборки

Готовая `.so` **не требует ничего**, кроме libstdc++, libm, libgcc_s и libc:

```bash
ldd out64/librdkafka_onec.so
```

```
	linux-vdso.so.1 (0x00007ffe5c5dd000)
	libstdc++.so.6 => /lib/x86_64-linux-gnu/libstdc++.so.6
	libm.so.6 => /lib/x86_64-linux-gnu/libm.so.6
	libgcc_s.so.1 => /lib/x86_64-linux-gnu/libgcc_s.so.1
	libc.so.6 => /lib/x86_64-linux-gnu/libc.so.6
	/lib64/ld-linux-x86-64.so.2
```

Ни `libssl`, ни `libcrypto`, ни `libcurl`, ни `libz`, ни `libzstd`, ни `libsasl2`.
Это не побочный эффект, а цель: на сервер 1С кладётся один файл, доустанавливать
на нём нечего, и версия OpenSSL на хосте на компоненту не влияет.

Быстрая проверка того же самого одной строкой (пустой вывод = хорошо):

```bash
ldd out64/librdkafka_onec.so | grep -E 'libssl|libcrypto|libcurl|libz\.'
```

`librt` в списке тоже нет, хотя `CMakeLists.txt` его линкует: в glibc ≥ 2.34 он
пустой и слился с libc.

Список выше — для сборки штатным `build-component-linux.sh`. У переносимой сборки
([§3.5](#35-переносимость-на-каких-системах-результат-загрузится)) зависимостей ещё
меньше: libstdc++ и libgcc влинкованы статически, остаются только `libpthread`,
`libdl`, `libm` и `libc`.

---

## 2. Требования

### 2.1 Linux x86_64

| Что | Зачем |
|---|---|
| `build-essential` | gcc, g++, make — ступени 1 и 2 |
| `cmake` ≥ 3.10 | ступень 2 (`cmake_minimum_required(VERSION 3.10)`) |
| `git` | клон репозитория и клон librdkafka на ступени 1 |
| `perl` | сборка OpenSSL на ступени 1 — её `Configure` написан на Perl |
| `wget` или `curl`, `ca-certificates` | mklove качает тарболлы OpenSSL/zlib/zstd/curl |
| glibc ≥ 2.31 | требование самой сборки |

```bash
sudo apt-get install build-essential cmake git perl wget ca-certificates
```

Ступень 1 сама проверяет наличие `git`, `gcc`, `g++`, `make`, `perl` и падает с
внятным сообщением, если чего-то нет. `wget`/`curl` она не проверяет — узнаете об
этом уже внутри сборки зависимости.

Дополнительно, **только для тестов и статанализа** (к сборке отношения не имеют):

| Что | Зачем |
|---|---|
| `docker` + `docker compose` v2 | брокер Kafka 4.x для TLS-теста (`tests/docker/`) |
| `cmake` ≥ 3.16 | проект тестов требует именно 3.16 |
| `clang-tidy`, `clang-tools` | `scripts/lint.sh` |
| `valgrind` | `tests/run-tests.sh --valgrind` |

JDK не нужен: `tests/docker/gen-certs.sh` при отсутствии локальных `keytool` и
`openssl` запускает их внутри образа Kafka.

### 2.2 Windows

| Что | Зачем |
|---|---|
| Visual Studio 2019 или новее, рабочая нагрузка **«Разработка классических приложений на C++»** | MSVC v142+; сборка с не-MSVC компилятором запрещена явным `FATAL_ERROR` в `CMakeLists.txt` |
| CMake ≥ **3.16** | `target_precompile_headers` (PCH для `src/stdafx.h`) появился в 3.16. В README указано 3.15 — этого мало. На практике вопрос не стоит: VS 2019/2022 приносит свой cmake ≥ 3.20 |
| Git for Windows | клон vcpkg и librdkafka |
| Доступ в интернет | `github.com` — клон vcpkg и librdkafka, `vcpkg.exe`, исходники OpenSSL/curl/zlib/zstd, CMake, Ninja, 7-Zip, PowerShell 7, Strawberry Perl. Кроме него — `download.qt.io` (jom), `mirror.msys2.org` или его зеркала (pkgconf, msys2-runtime), `www.nasm.us` (NASM; запасные адреса — `www.nasm.dev`, `vcpkg.github.io`). Через прокси — см. [§8.5](#85-curl-operation-failed-with-error-code-35-при-vcpkg-install) |
| ~3,5 ГБ на диске | `.build\vcpkg` после сборки x64 — 3,2 ГБ. Из них 1,9 ГБ — `downloads`: 0,5 ГБ скачанных архивов и 1,4 ГБ распакованных из них инструментов (один Strawberry Perl — почти 1 ГБ). `lib\win64` — ещё 106 МБ |

**vcpkg ставить руками не нужно.** `scripts\build-librdkafka-windows.ps1` сам
клонирует его в `.build\vcpkg` и бутстрапит (`bootstrap-vcpkg.bat -disableMetrics`),
если в каталоге нет `.git`. Готовый vcpkg можно переиспользовать параметром
`-VcpkgRoot`.

Perl, NASM и jom ставить тоже не нужно, хотя OpenSSL vcpkg собирает из исходников: всё
это он скачивает сам в `.build\vcpkg\downloads`.

---

## 3. Linux x86_64, по шагам

> **Если сборка уедет на сервер 1С — читайте [§3.5](#35-переносимость-на-каких-системах-результат-загрузится) прежде, чем что-либо собирать.**
> Собранное штатным способом на свежем дистрибутиве не загрузится ни на одном
> сервере 1С старше вашей машины, и платформа не скажет почему.

### 3.0 Клон

```bash
git clone https://github.com/arkhamvm/onec_librdkafka.git
cd onec_librdkafka
```

### 3.1 Ступень 1 — пересобрать librdkafka

**Нужно только при смене версии librdkafka.** Если `lib/linux64/librdkafka-static.a`
и `librdkafka++.a` на месте (а они закоммичены), переходите к [§3.3](#33-ступень-2--собрать-компоненту).

```bash
scripts/build-librdkafka-linux.sh v2.15.1
```

Аргумент — git-тег librdkafka; без аргумента берётся `v2.15.1`.

Что скрипт делает:

1. проверяет, что архитектура `x86_64` и что есть `git`, `gcc`, `g++`, `make`, `perl`;
2. определяет тип файловой системы, на которой лежит репозиторий, и выбирает рабочий
   каталог — см. [§3.2](#32-ловушка-ntfsexfat-читать-обязательно);
3. клонирует librdkafka нужного тега (`git clone --depth 1 --branch <тег>`), если
   рабочего каталога ещё нет;
4. конфигурирует:

   ```
   ./configure --install-deps --source-deps-only --enable-static --disable-lz4-ext
   ```

   * `--install-deps` — качать и собирать зависимости самому;
   * `--source-deps-only` — **запрещает** брать системные библиотеки; именно это
     делает архив самодостаточным и одинаковым на любом дистрибутиве;
   * `--disable-lz4-ext` — использовать lz4, вендоренный внутри librdkafka;

   > В этой строке **не хватает `--disable-gssapi`**, и на машине без
   > `libsasl2-dev` шаг упадёт с `Failed to install dependency libsasl2`.
   > Прочитайте [§8.2](#82-failed-to-install-dependency-libsasl2) **до** запуска,
   > а не после;
5. `make -j$(nproc)`;
6. копирует `src/librdkafka-static.a` и `src-cpp/librdkafka++.a` в `lib/linux64/`,
   а `src/rdkafka.h` и `src-cpp/rdkafkacpp.h` — в `src/`;
7. печатает `#define RD_KAFKA_VERSION` из положенного заголовка и содержимое `lib/linux64`.

Пункт 6 — обычный `cp`, он работает на любой файловой системе. Проблема всегда в
пункте 4, где зависимости делают `make install`.

**Время.** На 12 ядрах `./configure` с нуля (скачать и собрать OpenSSL 3.5.7,
zlib 1.3.2, zstd 1.5.7, curl 8.21.0) занял **~8,5 минут**; вместе со сборкой самой
librdkafka закладывайте 10–20 минут и несколько сотен мегабайт в рабочем каталоге.

**Заголовки и архивы обязаны быть из одной сборки.** Скрипт кладёт и то и другое за
один проход именно поэтому. Не переносите `.a` и `.h` по отдельности.

> **Не запускайте эту ступень от root.** mklove умеет доустанавливать системные
> пакеты через `apt install`, но только при `EUID == 0`
> (`mklove/modules/configure.base:378`). Под root он может подтянуть
> `libsasl2-dev` — и тогда в сборке появится Cyrus SASL и, вероятно, внешняя
> зависимость от `libsasl2.so`, то есть сломается свойство из [§1.4](#14-рантайм-зависимости-linux-сборки).
> Под обычным пользователем этот путь недоступен по построению.
> *(Вывод из кода mklove, на root не проверялось.)*
> Если сборку всё-таки делали под root — обязательно прогоните `ldd` из [§6](#6-как-убедиться-что-сборка-живая).

### 3.2 Ловушка NTFS/exFAT (читать обязательно)

Сборка зависимостей (OpenSSL, zlib, zstd, curl) заканчивается установкой в
промежуточный `DESTDIR`:

```
make DESTDIR="${destdir}" prefix=/usr install
```

`make install` и libtool вызывают `install(1)` с явными правами (`install -c -m 644 …`),
то есть делают `chmod`/`chown`. На разделах без POSIX-прав — NTFS и exFAT через
`ntfs-3g`/fuseblk, шары CIFS/SMB, 9p — это невозможно: владелец и режим у всего
дерева фиксированы опциями монтирования.

Так выглядит такой раздел:

```bash
findmnt -T . -no TARGET,FSTYPE,OPTIONS
```

```
/media/vladimir/Media fuseblk rw,relatime,user_id=0,group_id=0,allow_other,blksize=4096
```

И так он себя ведёт:

```bash
$ install -c -m 644 a b
install: установка прав доступа для «…/b»: Операция не позволена
$ echo $?
1
```

(`install: cannot change permissions of '…': Operation not permitted` в англоязычной
локали.) Обратите внимание: обычный `chmod` на том же файле возвращает 0 и делает
вид, что сработал, — падает именно `install`. Дальше рушится libtool, за ним сборка
зависимости, а configure сообщает `Failed to install dependency libssl` — про права
доступа в этом сообщении не будет ни слова.

**Скрипт это обрабатывает сам.** Он смотрит `stat -f -c '%T'` по корню репозитория
и, если файловая система из списка `fuseblk|ntfs|exfat|msdos|vfat|cifs|smb2|9p`,
уводит всю сборку в `$TMPDIR`:

| Где лежит репозиторий | Рабочий каталог сборки |
|---|---|
| нормальная ФС (ext4, xfs, btrfs…) | `<репозиторий>/.build/librdkafka-<тег>` |
| fuseblk / NTFS / exFAT / CIFS / 9p | `${TMPDIR:-/tmp}/onec-librdkafka-build/librdkafka-<тег>` |

В этом случае скрипт печатает строку вида:

```
>>> /media/… is on fuseblk (no POSIX permissions); building in /tmp/onec-librdkafka-build/librdkafka-v2.15.1 instead
```

Обратно на раздел репозитория попадают только готовые `.a` и `.h` — обычным `cp`,
который правами не занимается и на fuseblk работает.

Что из этого следует практически:

* в `$TMPDIR` должно быть несколько сотен мегабайт свободного места, и он должен
  быть на нормальной ФС (по умолчанию `/tmp` — да);
* `/tmp` может чиститься при перезагрузке, поэтому повторный запуск после
  перезагрузки клонирует librdkafka заново — это нормально;
* рабочий каталог **переживает** обычный повторный запуск, и это источник граблей
  из [§8.3](#83-mkl_patch-libcurl-failed-to-apply-patch);
* ступень 2 (CMake + make самой компоненты) на fuseblk работает нормально:
  там нет ни одного `install(1)`.

Проверить свою файловую систему:

```bash
stat -f -c '%T' .
```

### 3.3 Ступень 2 — собрать компоненту

```bash
scripts/build-component-linux.sh
```

Скрипт делает ровно четыре вещи: сносит `build/`, конфигурирует
`cmake -DCMAKE_BUILD_TYPE=Release ..`, собирает `cmake --build . -j$(nproc)`, затем
показывает содержимое `out64/` и вывод `ldd` по собранной `.so`. Зависимости он не
собирает: если `lib/linux64/librdkafka++.a` или `librdkafka-static.a` отсутствуют,
CMake останавливается с сообщением
`… is missing. Run scripts/build-librdkafka-linux.sh first.`

Ручной эквивалент:

```bash
mkdir -p build && cd build && rm -rf *
cmake -DCMAKE_BUILD_TYPE=Release ..
cmake --build . -j"$(nproc)"
```

### 3.4 Куда попадает артефакт

`CMakeLists.txt` задаёт выходной каталог как `${CMAKE_BINARY_DIR}/../out64` —
то есть **на уровень выше сборочного дерева**, а не внутрь него:

| Сборочное дерево | Готовая `.so` |
|---|---|
| `<репозиторий>/build` | `<репозиторий>/out64/librdkafka_onec.so` |
| `/tmp/mybuild` | `/tmp/out64/librdkafka_onec.so` |

Каноническая копия, которую грузят тесты, — `out64/librdkafka_onec.so` в корне
репозитория. Если собираете во временный каталог, выбирайте такой, чей родитель вам
не жалко: `cmake -B /tmp/work/comp` положит результат в `/tmp/work/out64/`.

Каталоги `out32/`, `out64/`, `build/`, `build-x64/`, `build-x86/` и `.build/`
перечислены в `.gitignore` — артефакты сборки в репозиторий не попадают
(закоммичены только `lib/linux64/*.a`).

### 3.5 Переносимость: на каких системах результат загрузится

**Это главная ловушка всей линуксовой сборки, и наступить в неё легко.**

`build-component-linux.sh` собирает тем компилятором и той glibc, что стоят на машине.
Собрали на свежей Ubuntu — получили `.so`, который на сервере 1С не загрузится вообще.
Платформа скажет ровно «ошибка подключения внешней компоненты», без единой подробности,
и искать причину вы будете долго.

Дело в версионировании символов glibc. Линковщик пишет в бинарник не `memcpy`, а
`memcpy@GLIBC_2.14`; если в файле есть ссылка на `GLIBC_2.38`, то на системе с glibc
2.31 такого символа нет и `dlopen` отказывает. Совместимость односторонняя: собранное
на старой glibc работает на новой, наоборот — никогда.

Проверить, что требует готовый файл:

```bash
objdump -T out64/librdkafka_onec.so | grep -oE 'GLIBC_[0-9]+\.[0-9]+' | sort -uV | tail -1
objdump -T out64/librdkafka_onec.so | grep -cE 'GLIBCXX|CXXABI'
```

| Собрано на | Требует | Ubuntu 20.04 | Ubuntu 22.04 | Debian 11 | Debian 10, RHEL 8 |
|---|---|---|---|---|---|
| Ubuntu 24.04, gcc 13 | `GLIBC_2.38`, `GLIBCXX_3.4.32` | ❌ | ❌ | ❌ | ❌ |
| контейнер 20.04, gcc 9.4 | `GLIBC_2.29`, GLIBCXX нет | ✅ | ✅ | ✅ | ❌ |

#### Сборка для поставки

```bash
scripts/build-linux-portable.sh
```

Скрипт поднимает `ubuntu:20.04` (glibc 2.31, gcc 9.4 — окружение, названное целевым в
апстримном README), собирает внутри **и librdkafka, и компоненту**, линкует libstdc++ и
libgcc статически, и только после проверки результата кладёт файлы в рабочее дерево:

```
out64/librdkafka_onec.so
lib/linux64/librdkafka-static.a
lib/linux64/librdkafka++.a
```

Нужен только `docker`; на хост ничего не ставится. Архивы librdkafka кешируются в
`.build/portable-<версия>/`, поэтому повторный запуск не пересобирает OpenSSL — это
основная часть времени. Первый прогон занимает минут пятнадцать.

Ключевое: **librdkafka пересобирается тем же старым компилятором**. Подложить готовые
`lib/linux64/*.a`, собранные на свежей машине, не выйдет — объектные файлы в них тянут
те же новые символы, и портируемость теряется на этапе линковки.

Скрипт сам проверяет то, ради чего существует, и падает, если результат не годится:

```
>>> verifying the result
    highest GLIBC required : 2.29  (limit 2.29)
    GLIBCXX / CXXABI refs  : 0  (must be 0)
    dynamic dependencies   :
      libpthread.so.0
      libdl.so.2
      libm.so.6
      libc.so.6
```

При превышении лимита он печатает **конкретные символы**, из-за которых планка поднялась,
и завершается с ненулевым кодом, ничего не записав в рабочее дерево.

Флаги:

| Флаг | Смысл |
|---|---|
| `--image <ref>` | другой базовый образ (см. ниже) |
| `--version <tag>` | версия librdkafka, по умолчанию `v2.15.1` |
| `--rebuild-deps` | игнорировать кеш и пересобрать librdkafka |
| `--max-glibc <x.y>` | порог проверки, по умолчанию `2.29` |

#### Если нужен Debian 10 или RHEL 8

У обоих glibc 2.28, а результат требует 2.29. Упирается это в `log2` и `pow`: glibc 2.29
ввела их новые версии, и libm подтягивает именно их. Лечится только более старой базой:

```bash
scripts/build-linux-portable.sh --image debian:10 --max-glibc 2.28
```

Такой прогон здесь не проверялся. Если `apt` в образе Debian 10 уже не отвечает
(репозитории переехали в archive.debian.org), придётся либо править `sources.list` в
образе, либо брать `centos:7` с `devtoolset`.

#### Чем это отличается от обычной сборки

`build-component-linux.sh` остаётся правильным выбором для разработки: он быстрее,
не требует docker и собирает ровно то, что можно тут же прогнать тестами. Но результат
его работы годится **только для этой машины**. Всё, что уезжает на сервер 1С или
кому-то передаётся, собирается `build-linux-portable.sh`.

---

## 4. Windows x64 / x86

Для **x64** раздел проверен исполнением (окружение — в начале документа), для x86 — нет.
Первая реальная сборка нашла в скриптах несколько дефектов: от скрипта, который
Windows PowerShell 5.1 не мог даже разобрать, до конфигурации librdkafka, с которой DLL
собралась бы без snappy и OIDC. Всё описанное ниже — поведение уже исправленных
скриптов; сообщения, которые выдаёт старая версия, — в
[§8.6](#86-ступень-1-на-windows-падает-на-старой-версии-скрипта).

### 4.1 Какую консоль открывать и почему

Откройте **«x64 Native Tools Command Prompt for VS 2022»** (или Developer PowerShell
для VS с той же архитектурой) из меню «Пуск». Для 32-битной сборки — **«x86 Native
Tools Command Prompt for VS»**.

Обычный `cmd`/`powershell` не годится: ни один из скриптов не ищет MSVC и не зовёт
`vcvarsall.bat`. Оба падают на первой же проверке `Get-Command cmake` или уже внутри
CMake, который не найдёт компилятор. Developer-консоль — это и есть тот самый
`vcvarsall`: она расставляет `PATH`, `INCLUDE`, `LIB` и `VSCMD_ARG_TGT_ARCH`.

То же самое из обычного `cmd` — вызвать `vcvars64.bat` перед скриптом. Проверочная
сборка шла именно так:

```bat
call "C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvars64.bat"
powershell -ExecutionPolicy Bypass -File scripts\build-librdkafka-windows.ps1
powershell -ExecutionPolicy Bypass -File scripts\build-component-windows.ps1
```

Сборка `x86` из `x64`-консоли (и наоборот) может пройти, но полагаться на это не
стоит: vcpkg берёт триплет из параметра `-Arch`, а хост-архитектура влияет на выбор
инструментов. Проще открыть правильную консоль.

### 4.2 Ступень 1 — librdkafka и её зависимости

```powershell
powershell -ExecutionPolicy Bypass -File scripts\build-librdkafka-windows.ps1
```

Параметры:

| Параметр | По умолчанию | Смысл |
|---|---|---|
| `-Version` | `v2.15.1` | git-тег librdkafka |
| `-Arch` | `x64` | `x64` или `x86` |
| `-VcpkgRoot` | `.build\vcpkg` | готовая установка vcpkg вместо бутстрапа |

Что происходит:

1. проверяются `git` и `cmake` на `PATH`;
2. vcpkg клонируется в `.build\vcpkg` и бутстрапится, если его там ещё нет;
3. ставятся зависимости триплетом `<arch>-windows-static`:

   ```
   vcpkg install zlib:x64-windows-static zstd:x64-windows-static openssl:x64-windows-static curl[core,ssl]:x64-windows-static --recurse
   ```

   Триплет `*-windows-static` — это статические библиотеки **и статическая CRT
   (`/MT`)**, ровно то, чем собирается сама компонента. Смешивать `/MT` и `/MD`
   в одном бинарнике нельзя;
4. клонируется librdkafka нужного тега в `.build\librdkafka-<тег>`;
5. сборочное дерево `.build\librdkafka-<тег>\build-<arch>` **удаляется**, если осталось от
   прошлого запуска. vcpkg не умеет переводить уже сконфигурированное дерево из
   manifest-режима в классический, а кэш прошлого запуска не должен решать, что попадёт
   в `lib\win64`;
6. CMake конфигурирует librdkafka с `RDKAFKA_BUILD_STATIC=ON`, `WITH_SSL/CURL/ZLIB/ZSTD/SASL=ON`,
   `ENABLE_LZ4_EXT=OFF`, тулчейном vcpkg и
   `-DCMAKE_POLICY_DEFAULT_CMP0091=NEW -DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded`.
   `CMP0091=NEW` здесь обязателен: librdkafka объявляет
   `cmake_minimum_required(VERSION 3.5)`, при старом поведении политики
   `CMAKE_MSVC_RUNTIME_LIBRARY` просто игнорируется и вы получите `/MD`-сборку,
   которая потом не слинкуется с `/MT`-зависимостями.

   Ещё несколько флагов и зачем они:

   | Флаг | Что будет без него |
   |---|---|
   | `"-DVCPKG_TARGET_TRIPLET=$Triplet"` — в кавычках целиком | PowerShell не раскрывает переменную в аргументе вида `-DИМЯ=$var`, и CMake получает буквальную строку `$Triplet`: тулчейн ищет зависимости в несуществующем `<vcpkg>\installed\$Triplet` и не находит их. Сообщение `Invalid triplet name` выдавал старый скрипт, где ещё работал manifest-режим и тулчейн сам запускал `vcpkg install` ([§8.6](#86-ступень-1-на-windows-падает-на-старой-версии-скрипта)) |
   | `-DVCPKG_MANIFEST_MODE=OFF` | в корне librdkafka лежит `vcpkg.json`, тулчейн включает manifest-режим и ставит в `build-<arch>\vcpkg_installed` второй комплект зависимостей других версий. librdkafka компилируется против него, а в `lib\win64` копируются библиотеки из классического дерева — заголовки и архивы из разных сборок |
   | `-DVCPKG_INSTALLED_DIR=<vcpkg>\installed` | в текущем скрипте ничего: при `VCPKG_MANIFEST_MODE=OFF` и свежем дереве `vcpkg.cmake` сам берёт `<vcpkg>\installed`. Это страховка для переиспользованного дерева, например при ручном запуске CMake: значение из `CMakeCache.txt` `vcpkg.cmake` предпочитает умолчанию, а явный `-D` его перезаписывает, и компиляция смотрит в то же дерево, из которого копируются библиотеки |
   | `-DWITHOUT_WIN32_CONFIG=OFF` | на `_WIN32` `rd.h` берёт флаги возможностей только из `src\win32_config.h`. По умолчанию CMake этот блок выключает и подставляет неполную замену, где `WITH_SNAPPY=0`, а `WITH_CURL`, `WITH_OAUTHBEARER_OIDC` и `WITH_HDRHISTOGRAM` не определены. DLL при этом собирается, но snappy не работает: продюсер отвергает `compression.codec=snappy` (`snappy not enabled at build time`), а консьюмер не распаковывает snappy-батчи (`Decompression (codec 0x2) … failed: Local: Not implemented`). Настройки OIDC такая DLL тоже отвергает |
   | `-DWITH_HDRHISTOGRAM=ON` | блок из `win32_config.h` включает `WITH_HDRHISTOGRAM`, а `rdhdrhistogram.c` CMake компилирует только при одноимённой переменной — её собственная проверка через libm на Windows всегда проваливается. Итог — неразрешённые `rd_hdr_histogram_*` |
7. `cmake --build … --config Release --parallel`;
8. **`lib\win64\*.lib` очищается целиком** и туда кладутся свежие файлы. Если какой-то
   зависимости в vcpkg нет ни под одним из известных скрипту имён, он останавливается
   ([§4.3](#43-что-оказывается-в-libwin64)).

### 4.3 Что оказывается в `lib\win64`

| Файл | Откуда |
|---|---|
| `rdkafka.lib` | сборка librdkafka |
| `rdkafka++.lib` | сборка librdkafka |
| `libssl.lib` | `.build\vcpkg\installed\x64-windows-static\lib` |
| `libcrypto.lib` | оттуда же |
| `libcurl.lib` | оттуда же |
| `zlib.lib` | оттуда же, файл `zs.lib` |
| `zstd.lib` | оттуда же |

Плюс перезаписываются `src\rdkafka.h` и `src\rdkafkacpp.h`. Они приезжают из клона с
переводами строк CRLF, и `git status` может показать их изменёнными, хотя содержимое
совпадает с закоммиченным — `git diff` пуст.

vcpkg переименовывает архивы между версиями портов: zlib 1.3.2 ставит `zs.lib`, а не
`zlib.lib`, как порты до него. Скрипт ищет каждую зависимость под всеми известными
ему именами и кладёт под одним — тем, что в таблице. Если не нашлась ни под одним,
скрипт останавливается, а не доходит до `>>> done`:

```
vcpkg dependency zlib.lib not found in <vcpkg>\installed\x64-windows-static\lib (looked for: zs.lib, zlib.lib)
```

`CMakeLists.txt` каталог глобом не подхватывает: он линкует ровно эти семь имён, а
любой другой `.lib` в `lib\win64` считает остатком чужой сборки и останавливает
конфигурацию.

Если vcpkg переименует зависимость снова, ступень 1 остановится на сообщении выше.
Проще всего добавить новое имя в список кандидатов (`$deps`) в
`scripts\build-librdkafka-windows.ps1` и запустить ступень 1 заново: файл ляжет в
`lib\win64` под прежним именем, и ни `CMakeLists.txt`, ни `RDKAFKA_WIN_EXTRA_LIBS` не
понадобятся. `RDKAFKA_WIN_EXTRA_LIBS` ([§9.1](#91-компонента)) нужен, только если архив под
новым именем положили в `lib\win64` руками.

Для 32 бит всё то же самое, но в `lib\win32`:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\build-librdkafka-windows.ps1 -Arch x86
```

**Сколько это идёт.** Замер на AMD Ryzen 5 3600 (12 потоков), x64, холодный кэш vcpkg:

| Этап | Время |
|---|---|
| `vcpkg install` | 8 мин, из них OpenSSL — 7 мин, curl — 31 с, zstd — 17 с, zlib — 9 с |
| конфигурация и сборка librdkafka | ~1,5 мин |
| ступень 2, компонента | ~30 с |

Исходники портов к этому моменту были уже скачаны, но сеть в замер всё же попала:
Strawberry Perl (~300 МБ), NASM и jom vcpkg скачивает прямо во время сборки OpenSSL, и
на загрузку через прокси с распаковкой ушло около двух минут из семи
([§8.5](#85-curl-operation-failed-with-error-code-35-при-vcpkg-install)).
Повторный запуск, когда пакеты уже в `installed\`, vcpkg проходит за секунды, и
ступень 1 сводится к пересборке librdkafka.

### 4.4 Почему `.lib` не лежат в репозитории

Так решено намеренно — см. `lib/win64/README.md` и `lib/win32/README.md`.

Заголовки `src/rdkafka.h`, `src/rdkafkacpp.h` **и** `.lib` в `lib/win64` обязаны быть
из одной сборки librdkafka. Если закоммитить `.lib` от 2.3.0, а заголовки обновить до
2.15.1, компоновщик, скорее всего, промолчит: имена C-функций совпадают, C++-обёртка
тоже не меняет манглинг настолько, чтобы это поймать. А вот структуры между версиями
разъехались — и код, собранный по новым заголовкам, начнёт писать в объекты,
разложенные по-старому. Это не ошибка сборки, это порча памяти в проде на миллионах
сообщений.

Поэтому:

* `.gitignore` не пускает `.lib` из `lib/win64` и `lib/win32` в индекс — в git там лежат
  только `README.md`;
* ступень 1 на Windows **стирает** каталог перед установкой
  (`Get-ChildItem -Filter *.lib | Remove-Item -Force`), чтобы от прошлой версии ничего
  не осталось;
* заголовки и `.lib` кладутся одним и тем же запуском скрипта.

Если `lib\win64` пуст, CMake останавливается сам:
`No .lib files in <путь>. Run scripts/build-librdkafka-windows.ps1 first.`

### 4.5 Ступень 2 — собрать компоненту

```powershell
powershell -ExecutionPolicy Bypass -File scripts\build-component-windows.ps1
```

Скрипт сносит `build-x64\`, конфигурирует и собирает:

```powershell
cmake -S <репозиторий> -B build-x64 -A x64 `
    -DCMAKE_POLICY_DEFAULT_CMP0091=NEW `
    -DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded
cmake --build build-x64 --config Release --parallel
```

`/MT` здесь тот же и по той же причине, что и на ступени 1: статическая CRT означает,
что готовый DLL кладётся в установку 1С без распространяемого пакета VC++.

Для 32 бит: `-Arch x86` (сборочное дерево `build-x86\`, выход в `out32\`).

### 4.6 Где именно оказывается DLL

`CMakeLists.txt` задаёт выходной каталог как `${CMAKE_BINARY_DIR}/../out64`. Генератор
Visual Studio — **многоконфигурационный**, и CMake дописывает к такому пути имя
конфигурации. Значит:

| Что | Путь |
|---|---|
| Сборочное дерево | `build-x64\` |
| DLL | `out64\Release\rdkafka_onec.dll` |
| Import-библиотека | `out64\Release\rdkafka_onec.lib` |
| То же для x86 | `out32\Release\rdkafka_onec.dll` |

В README указано `out64\rdkafka_onec.dll` — это базовый каталог; сам файл лежит на
уровень глубже, в подкаталоге конфигурации. Финальный `Get-ChildItem out64` в скрипте
покажет именно каталог `Release`, а не файл.

Под MSVC это подтверждено — ближе к концу вывода MSBuild (после неё идёт только
`Building Custom Rule …/CMakeLists.txt` цели `ALL_BUILD`) есть строка:

```
rdkafka_onec.vcxproj -> <репозиторий>\out64\Release\rdkafka_onec.dll
```

Строкой выше будет предупреждение, и оно ожидаемое:

```
warning LNK4070: директива /OUT:kafka_export.dll в .EXP отличается от имени выходного файла "…\out64\Release\rdkafka_onec.dll"; директива игнорируется
```

`src/exports.def` унаследовал от апстрима `LIBRARY "kafka_export"`. На подключение это не
влияет: платформа открывает DLL по пути к файлу и ищет пять функций по имени.

---

## 5. 32 бита

| Платформа | Поддержка |
|---|---|
| Linux x86 (32 бита) | **не поддерживается** |
| Windows x86 (32 бита) | поддерживается, `-Arch x86` |

**Linux x86.** `lib/linux32` не содержит сборки librdkafka 2.15.1, и CMake
останавливается явным сообщением (`CMakeLists.txt:79`):

```
32-bit Linux is not supported by this fork: lib/linux32 holds no librdkafka 2.15.1
build. Run scripts/build-librdkafka-linux.sh with a 32-bit toolchain (gcc-multilib)
to produce one.
```

Ступень 1 отказывается ещё раньше, если архитектура машины не `x86_64`:
`error: this script targets x86_64; got <arch>`.

На машине без `gcc-multilib` до этого сообщения дело не доходит вовсе: CMake падает
раньше, на проверке компилятора (`cannot find crti.o`, `cannot find -lgcc`) — так
получилось при попытке сконфигурировать проект с `-DCMAKE_C_FLAGS=-m32
-DCMAKE_CXX_FLAGS=-m32`. Сообщение про `lib/linux32` появится только тогда, когда
32-битный тулчейн есть и CMake дошёл до `CMAKE_SIZEOF_VOID_P == 4`.

**Windows x86.** Отдельного ограничения нет: обе ступени принимают `-Arch x86`,
зависимости ставятся триплетом `x86-windows-static`, `.lib` попадают в `lib\win32`,
результат — в `out32\Release\rdkafka_onec.dll`.

Разрядность компоненты обязана совпадать с разрядностью процесса 1С, который её
грузит.

---

## 6. Как убедиться, что сборка живая

«Скомпилировалось» не значит «работает». Минимальный набор — три проверки
(§6.1–6.3), каждая занимает секунды. Команды в §6.1–6.5 — для Linux; Windows — в
[§6.6](#66-проверки-готового-dll-на-windows).

### 6.1 На месте ли пять экспортов 1С

```bash
nm -D --defined-only out64/librdkafka_onec.so | grep -E ' (GetClassObject|DestroyObject|GetClassNames|SetPlatformCapabilities|GetAttachType)$'
```

```
00000000001f10c0 T DestroyObject
00000000001f1110 T GetAttachType
00000000001f10f0 T GetClassNames
00000000001f0da0 T GetClassObject
00000000001f1100 T SetPlatformCapabilities
```

Ровно пять строк, каждая с типом `T`. Это те функции, которые ищет платформа 1С; без
любой из них компонента не подключится.

`grep` здесь не украшение: библиотека экспортирует ~13 000 символов, потому что
OpenSSL, zlib, zstd и curl вкомпилированы внутрь. Голый
`nm -D --defined-only out64/librdkafka_onec.so` выдаст простыню от `a2d_ASN1_OBJECT`
и ниже.

### 6.2 Нет ли внешних зависимостей от криптографии

```bash
ldd out64/librdkafka_onec.so
```

Ожидается ровно то, что в [§1.4](#14-рантайм-зависимости-linux-сборки):
`libstdc++`, `libm`, `libgcc_s`, `libc`, `linux-vdso` и `ld-linux`. Появление в этом
списке `libssl`, `libcrypto`, `libcurl`, `libz`, `libzstd` или `libsasl2` означает,
что librdkafka собралась против системных библиотек, а не из исходников, — сборку
надо переделать (см. [§8.2](#82-failed-to-install-dependency-libsasl2) и
предупреждение про root в [§3.1](#31-ступень-1--пересобрать-librdkafka)).

### 6.3 Смоук-тест

Настоящая проверка: `.so` грузится через настоящий Native API, все три класса
проходят полный жизненный цикл, сверяется таблица методов, подтверждается версия
librdkafka и то, что конфигурация TLS вообще принимается.

```bash
cmake -S tests -B tests/build
cmake --build tests/build -j
./tests/build/smoke_test out64/librdkafka_onec.so
```

```
-------------------------------------------------------------
smoke: 944 check(s), 0 failure(s)
```

Код возврата 0. Брокер и сеть не нужны, всё занимает доли секунды.

### 6.4 Полный прогон

```bash
tests/run-tests.sh
```

Собирает тесты, гоняет смоук, поднимает докерный брокер Kafka 4.3.1 с TLS-слушателем,
прогоняет TLS-тест и гасит брокер в любом исходе.

Только смоук, без docker:

```bash
tests/run-tests.sh --no-docker
```

Такой прогон **намеренно** завершается кодом 3, а не 0: смоук прошёл, но TLS не
проверялся, а ради него набор и написан. Проверено — выход 3 и объяснение в конце
вывода. Если вам действительно нужен только смоук: `--no-docker --allow-smoke-only`.

Всё остальное — устройство набора, внешний брокер, санитайзеры, soak, коды выхода,
переменные окружения, разбор частых отказов — в [tests/README.md](tests/README.md) и
[tests/docker/README.md](tests/docker/README.md). Здесь это не дублируется.

### 6.5 Статический анализ

К сборке не относится, но запускается оттуда же:

```bash
scripts/lint.sh              # отчёт
scripts/lint.sh --tidy-only  # без scan-build, заметно быстрее
```

Гейт: блокирующими считаются диагностики clang-tidy уровня `error`, все
`clang-analyzer-*` и все отчёты scan-build; их на дереве должно быть ноль. Остальное
(ownership, CERT, performance, narrowing) печатается как advisory и прогон не валит,
если не передан `--strict`. Рабочий каталог — вне репозитория,
`${TMPDIR:-/tmp}/onec-librdkafka-lint`, переопределяется `ONEC_LINT_DIR`.

### 6.6 Проверки готового DLL на Windows

Из Developer-консоли ([§4.1](#41-какую-консоль-открывать-и-почему)) — там есть `dumpbin`.

**Экспорты.**

```bat
dumpbin /nologo /exports out64\Release\rdkafka_onec.dll
```

Среди них должны быть пять функций 1С: `DestroyObject`, `GetAttachType`,
`GetClassNames`, `GetClassObject`, `SetPlatformCapabilities`. Всего экспортов 83,
остальные 78 — `cJSON_*`: встроенный в librdkafka `cJSON.h` на Windows помечает свои
функции `__declspec(dllexport)`, и компоновщик выносит их наружу. Платформе они не
мешают — она ищет пять имён через `GetProcAddress`. Функций `rd_kafka_*` среди
экспортов нет.

**Зависимости.**

```bat
dumpbin /nologo /dependents out64\Release\rdkafka_onec.dll
```

```
    WS2_32.dll
    CRYPT32.dll
    Secur32.dll
    bcrypt.dll
    ADVAPI32.dll
    USER32.dll
    IPHLPAPI.DLL
    KERNEL32.dll
```

Только системные DLL — это Windows-аналог [§6.2](#62-нет-ли-внешних-зависимостей-от-криптографии).
`VCRUNTIME140.dll`, `MSVCP140.dll` или `api-ms-win-crt-*` в списке означали бы, что CRT
собрана не `/MT` и рядом с 1С понадобится распространяемый пакет VC++; `libssl-3-x64.dll`,
`libcrypto-3-x64.dll`, `libcurl.dll`, `z.dll` (у портов zlib до 1.3.2 — `zlib1.dll`) или
`zstd.dll` — что зависимости пришли не из `*-windows-static`.

**Набор возможностей librdkafka.**

```bat
findstr /m /c:"SSL ZLIB SNAPPY ZSTD CURL SASL_SCRAM SASL_OAUTHBEARER PLUGINS HDRHISTOGRAM" out64\Release\rdkafka_onec.dll
findstr /m /c:"snappy not enabled at build time" out64\Release\rdkafka_onec.dll
```

Первая команда напечатает имя файла в **любой** MSVC-сборке librdkafka: `BUILT_WITH` в
`src\win32_config.h` задан вне `#ifndef WITHOUT_WIN32_CONFIG` и от фактических флагов не
зависит, так что она лишь показывает, что код librdkafka попал в DLL. Набор
возможностей проверяет вторая — она не должна напечатать ничего. Если она что-то
нашла, librdkafka собрана без `-DWITHOUT_WIN32_CONFIG=OFF`
([§4.2](#42-ступень-1--librdkafka-и-её-зависимости)): такая DLL не прочитает
snappy-сообщения и не примет настройки OIDC.

**Смоук-тест.** Набор `tests/` в репозитории на Windows не собирается: он написан под
Linux (`dlfcn.h`, `fork()`, `readlink(/proc/self/exe)`). Для проверки этой сборки
смоук-тест был перенесён на Windows во временной копии вне репозитория: `dl*` заменены
на `LoadLibraryExW`/`GetProcAddress`, изоляция сценариев через `fork()` — на перезапуск
exe через `CreateProcessW`. Кроме того, порт изменил одну проверку — версию librdkafka:
в исходном наборе отсутствие `rd_kafka_version_str` в экспортах даёт провал, а порт
засчитывает четыре её проверки как пропуск, с отдельным счётчиком и отдельной строкой
итога. Остальные проверки не менялись. Результат на `out64\Release\rdkafka_onec.dll`:

```
smoke: 940 check(s), 0 failure(s)
smoke: 4 check(s) skipped as not applicable on this platform
```

940 выполненных плюс 4 пропущенные — те же 944 проверки, что смоук-тест даёт на Linux
([§6.3](#63-смоук-тест)). Пропуски — потому что Windows-DLL не экспортирует
`rd_kafka_version_str()` и `rd_kafka_version()`.

Версию подтвердила отдельная программа-проба, тоже вне репозитория. Она включает у
`KafkaConsumer` лог компоненты (`SetLogFilePath`) и `debug=conf`, и тогда librdkafka
пишет туда отладочную строку `INIT` — без `debug` её нет:
`librdkafka v2.15.1 (0x20f01ff)`, и в ней же `builtin.features`:
`gzip, snappy, ssl, sasl, regex, lz4, sasl_gssapi, sasl_plain, sasl_scram, plugins, zstd, sasl_oauthbearer, http, oidc`.
TLS-проверки смоук-теста прошли все: несуществующий `ssl.ca.location` все три класса
отвергают ошибкой OpenSSL 3.

---

## 7. Установка собранной компоненты в 1С

Здесь ровно то, что можно подтвердить по репозиторию. Процедуры подключения
компоненты в конфигурации в репозитории нет, и придумывать её этот документ не будет.

**Что известно точно:**

| Факт | Откуда |
|---|---|
| Файл для Linux x64 — `out64/librdkafka_onec.so`, для Windows x64 — `out64\Release\rdkafka_onec.dll` | `CMakeLists.txt`, скрипты сборки |
| Разрядность файла обязана совпадать с разрядностью процесса 1С | `CMakeLists.txt` выбирает `lib/linux64` / `lib/win64` по `CMAKE_SIZEOF_VOID_P` |
| Компонента реализует Native API **2.0** (`GetInfo()` возвращает 2000) | проверяется смоук-тестом |
| `GetAttachType()` возвращает `eCanAttachAny` | проверяется смоук-тестом |
| Компонента регистрирует ровно три класса: **`KafkaProducer`**, **`KafkaConsumer`**, **`KafkaAdminClient`** | `GetClassNames()`, проверяется смоук-тестом |
| Ни один файл не нужно доустанавливать рядом с компонентой | `ldd`, [§1.4](#14-рантайм-зависимости-linux-сборки); на Windows — `dumpbin /dependents`, [§6.6](#66-проверки-готового-dll-на-windows) |

Список классов можно получить и из самого бинарника — именно его вернёт платформе
`GetClassNames()`:

```bash
strings -a -el out64/librdkafka_onec.so | grep -m1 '^|Kafka'
```

```
|KafkaProducer|KafkaConsumer|KafkaAdminClient
```

`-el` здесь обязателен: строки, пересекающие ABI 1С, лежат в UTF-16
(`WCHAR_T` — это `char16_t`), и обычный `strings` их не видит.

### 7.1 ZIP-макет внешней компоненты

Платформа грузит компоненту либо файлом с диска, либо макетом внешней компоненты — это
ZIP, в корне которого лежит `MANIFEST.xml`, а рядом бинарники под каждую ОС и
разрядность. Макет удобнее тем, что один объект конфигурации работает и на
Linux-сервере, и на Windows-клиенте: нужный файл платформа выбирает сама.

Собирает макет `scripts/package-addin.sh`:

```bash
bash scripts/package-addin.sh
```

Windows-DLL и Linux-`.so` собираются на разных машинах, поэтому обычный запуск — это
скопировать DLL и указать её явно:

```bash
bash scripts/package-addin.sh --win64 /mnt/share/rdkafka_onec.dll
```

Результат — `dist/rdkafka_onec.zip` (каталог `dist/` в `.gitignore`). Вывод прогона на
двух x64-бинарниках, дословно:

```
>>> <репозиторий>/dist/rdkafka_onec.zip
Archive:  <репозиторий>/dist/rdkafka_onec.zip
  Length      Date    Time    Name
---------  ---------- -----   ----
      344  2026-09-18 15:16   MANIFEST.xml
 18535072  2026-09-18 15:16   rdkafka_onec_linux_x86_64.so
  9154560  2026-09-18 15:16   rdkafka_onec_win_x86_64.dll
---------                     -------
 27689976                     3 files

<?xml version="1.0" encoding="UTF-8"?>
<bundle xmlns="http://v8.1c.ru/8.2/addin/bundle" name="rdkafka_onec">
    <component os="Linux" arch="x86_64" path="rdkafka_onec_linux_x86_64.so" type="native" buildType="release"/>
    <component os="Windows" arch="x86_64" path="rdkafka_onec_win_x86_64.dll" type="native" buildType="release"/>
</bundle>
```

Платформа, которой не передали бинарник, в манифест просто не попадает: на ней
компонента не загрузится, а остальные продолжат работать. Итоговый состав архива и сам
манифест скрипт печатает в конце — недостающая платформа видна сразу.

**Linux-бинарник в макет берите из `scripts/build-linux-portable.sh`, а не из
`build-component-linux.sh`.** Путь у них один и тот же, `out64/librdkafka_onec.so`, и
скрипт упаковки подхватит любой — но обычная сборка несёт glibc своей машины и на
сервере 1С постарше не загрузится вовсе, а платформа скажет только «ошибка подключения
внешней компоненты» ([§3.5](#35-переносимость-на-каких-системах-результат-загрузится)).

Флаги:

| Флаг | Смысл |
|---|---|
| `--name NAME` | имя бандла в `MANIFEST.xml`, имя ZIP и основа имён файлов внутри архива. Допустимы `[A-Za-z0-9._-]`, первый символ — буква, цифра или `_`. По умолчанию `rdkafka_onec` |
| `--out FILE` | куда положить архив; относительный путь считается от текущего каталога. По умолчанию `dist/<name>.zip` |
| `--build-type release`&#124;`developer` | атрибут `buildType` у каждой компоненты. По умолчанию `release` |
| `--linux64 PATH`<br>`--win64 PATH`<br>`--win32 PATH` | явные пути к бинарникам. Указанный явно и не найденный файл — ошибка; ненайденный при автопоиске — просто платформа вне архива |

Ограничение на `--name` — не придирка: имя попадает разом в значение XML-атрибута, в имя
файла внутри архива и в аргумент `zip`. Набор символов выбран так, чтобы быть безопасным
во всех трёх местах сразу, вместо трёх разных экранирований. Имя с ведущим дефисом
`zip` прочитал бы как свой ключ (`-x` — «исключить») и молча выбросил бы файл из архива,
оставив манифест, который обещает то, чего внутри нет.

Автопоиск для Linux смотрит `out64/librdkafka_onec.so`, затем `out64/Release/librdkafka_onec.so`;
для Windows — сначала `out64\Release\rdkafka_onec.dll`, потом `out64\rdkafka_onec.dll`;
то же для `out32`. Подкаталог конфигурации проверяется потому, что генератор Visual Studio
многоконфигурационный и дописывает имя конфигурации к выходному каталогу
([§4.6](#46-где-именно-оказывается-dll)), а тот же CMake под Ninja или make кладёт файл
прямо в `out64`. Linux 32 бита не ищется вовсе — он не поддерживается ([§5](#5-32-бита)).

**Что проверяется до упаковки.** Оба отказа ловятся здесь, а не на машине заказчика в
момент `ПодключитьВнешнююКомпоненту`:

* формат и разрядность читаются из самого файла (`file`), а не берутся из ключа:
  64-битная DLL, переданная в `--win32`, отвергается с указанием, что в ней на самом
  деле лежит;
* бинарник обязан быть именно этой компонентой — в нём ищется UTF-16 строка
  `|KafkaProducer|KafkaConsumer|KafkaAdminClient`, та самая, которую вернёт платформе
  `GetClassNames()` (см. `strings -el` выше). Чужой `.so`, случайно оказавшийся в
  `out64`, так не пройдёт.

Если `file` или `strings` в системе нет, соответствующая проверка пропускается:
упаковка не должна падать из-за отсутствия диагностической утилиты. По той же причине
пропускается и `unzip`, которым печатается состав архива, — в Debian и Alpine это
отдельный от `zip` пакет, и собранный макет не должен превращаться в ненулевой код
возврата только потому, что показать его содержимое нечем. Обязателен ровно один
внешний инструмент — `zip`; без него скрипт сразу останавливается.

**`include/MANIFEST.xsd` для проверки не годится**, и это стоит знать заранее. Схема из
SDK 1С в том виде, в каком её поставляет 1С, не компилируется — дефекта четыре:

* каждый `<xs:documentation>` открыт дважды и ни разу не закрыт, так что файл не
  является даже well-formed XML;
* в каждом `<xs:simpleType>` перечисления лежат без обёртки `<xs:restriction>`;
* все семь ссылок на типы (`type="Component"`, `type="OSType"`, `type="ArchType"`,
  `type="ComponentType"`, `type="ClientType"`, `type="buildTypeEnum"`,
  `type="codeTypeEnum"`) не квалифицированы, хотя сами типы объявлены в целевом
  пространстве имён, — и потому не разрешаются;
* не объявлено ни одного элемента верхнего уровня, то есть корня, против которого
  проверялся бы документ, в схеме нет.

Файл оставлен в `include/` ровно таким, каким его поставляет 1С. Сгенерированный
`MANIFEST.xml` скрипт проверяет только на well-formedness (`xmllint --noout`) — и то
скорее для страховки: значения атрибутов либо зафиксированы в самом скрипте, либо
построены из `--name`, а тот ограничен символами, которым экранирование в XML не нужно.

### 7.2 Чего в репозитории нет

* нет ни одной строки кода на встроенном языке и ни одного описания того, как
  компонента подключается — ни в README, ни в скриптах;
* нет инструкции по размещению файла на сервере 1С.

Порядок подключения внешней компоненты (макет в конфигурации либо файл на диске,
`ПодключитьВнешнююКомпоненту`, `Новый("AddIn.<ИмяПодключения>.KafkaProducer")`,
требования к каталогу на сервере и к правам) задаёт платформа, а не этот репозиторий.
Смотрите документацию 1С: **«Технология создания внешних компонент»** на ИТС
(раздел про Native API) — заголовки из `include/` взяты именно из этого SDK.

**`example.epf`** — внешняя обработка 1С в корне репозитория (19 МБ), пришла из
самого первого коммита вместе с апстримом `skalkindv/onec_librdkafka`. Это
бинарный контейнер: ни README, ни один скрипт её не упоминают и не собирают,
никакой текстовой выгрузки в репозитории нет. Открывайте её Конфигуратором или
«1С:Предприятием» — это единственный источник в репозитории, показывающий вызовы
методов компоненты из встроенного языка. Считать её актуальной для текущей
версии компоненты нельзя: она не менялась с первого коммита, тогда как список
методов с тех пор изменился (`tests/README.md`, раздел «Расхождения с корневым
README.md», перечисляет методы, не попавшие в документацию).

---

## 8. Диагностика

Здесь только те отказы, которые уже случались на этой сборке. Проблемы тестов и
брокера — в [tests/README.md](tests/README.md).

### 8.1 `install: Operation not permitted` при сборке зависимостей

**Симптом.** Ступень 1 падает где-то внутри сборки OpenSSL, zlib, zstd или curl.
В логе — libtool или `install`:

```
install: установка прав доступа для «…»: Операция не позволена
libtool: install: … Operation not permitted
```

Дальше configure сообщает что-нибудь вроде `Failed to install dependency libssl`,
и про права доступа в этом сообщении уже ни слова.

**Причина.** Сборку запустили в каталоге на файловой системе без POSIX-прав:
NTFS или exFAT через `ntfs-3g`/fuseblk, шара CIFS/SMB, 9p. `make install`
у зависимостей вызывает `install -c -m …`, то есть `chmod`, а на таком разделе
владелец и режим фиксированы опциями монтирования.

**Проверка.**

```bash
stat -f -c '%T' .
findmnt -T . -no TARGET,FSTYPE,OPTIONS
```

`fuseblk`, `ntfs`, `exfat`, `cifs`, `9p` в ответе — это оно.

**Что делать.** Запускать `scripts/build-librdkafka-linux.sh`, а не configure руками:
скрипт определяет такую ФС сам и уводит сборку в `${TMPDIR:-/tmp}`, оставляя на
«плохом» разделе только копирование готовых `.a` и `.h`. Подробно — [§3.2](#32-ловушка-ntfsexfat-читать-обязательно).

Если собираете вручную — держите рабочее дерево на ext4/xfs/btrfs, а в репозиторий
копируйте только результат.

### 8.2 `Failed to install dependency libsasl2`

**Симптом.** Ступень 1 останавливается на конфигурации:

```
Failed to install dependency libsasl2
```

**Причина.** `libsasl2` (Cyrus SASL, он же GSSAPI/Kerberos) — единственная
зависимость librdkafka, **для которой у mklove нет рецепта сборки из исходников**.
Её модуль так и подписан:

```
# libsasl2 support (for GSSAPI/Kerberos), without source installer.
```

Флаг `--source-deps-only`, ради которого всё и затевалось, запрещает брать
системные библиотеки, а собрать эту из исходников mklove не умеет. Поставить её
пакетом он тоже не может: путь через `apt install` включается только при `EUID == 0`
(`mklove/modules/configure.base:378` — файл в дереве librdkafka, не в этом
репозитории), и под обычным пользователем не работает.
Остаётся жёсткий отказ — причём независимо от того, что `--enable-gssapi` по
умолчанию стоит в `try`: при `--source-deps-only` неустановленная зависимость
всегда фатальна (`configure.base:588–592`).

**Что делать.** Добавить `--disable-gssapi` к строке configure:

```bash
./configure --install-deps --source-deps-only --enable-static --disable-lz4-ext --disable-gssapi
```

Именно так собраны `.a`, лежащие в `lib/linux64` сейчас:

```bash
strings -a lib/linux64/librdkafka-static.a | grep -m1 STATIC_LINKING
```

```
STATIC_LINKING GCC GXX PKGCONFIG INSTALL GNULD LDS C11THREADS LIBDL PLUGINS ZLIB SSL
ZSTD CURL HDRHISTOGRAM SYSLOG SNAPPY SOCKEM SASL_SCRAM SASL_OAUTHBEARER
OAUTHBEARER_OIDC CRC32C_HW
```

`SASL_CYRUS` в строке нет — значит GSSAPI выключен.

> В `scripts/build-librdkafka-linux.sh` на момент написания этого документа
> `--disable-gssapi` в строке configure **отсутствует**. Пока его туда не добавили,
> ступень 1 на машине без `libsasl2-dev` упадёт. Обходной путь: доконфигурировать и
> дособрать руками в том рабочем каталоге, который скрипт уже подготовил, и
> скопировать результат так же, как это делает скрипт:
>
> ```bash
> cd "${TMPDIR:-/tmp}/onec-librdkafka-build/librdkafka-v2.15.1"
> ./configure --install-deps --source-deps-only --enable-static --disable-lz4-ext --disable-gssapi
> make -j"$(nproc)"
> cp src/librdkafka-static.a  <репозиторий>/lib/linux64/librdkafka-static.a
> cp src-cpp/librdkafka++.a   <репозиторий>/lib/linux64/librdkafka++.a
> cp src/rdkafka.h            <репозиторий>/src/rdkafka.h
> cp src-cpp/rdkafkacpp.h     <репозиторий>/src/rdkafkacpp.h
> ```
>
> (Путь к рабочему каталогу — из [§3.2](#32-ловушка-ntfsexfat-читать-обязательно);
> на нормальной ФС это `<репозиторий>/.build/librdkafka-v2.15.1`.)

**Чем за это заплачено.** Аутентификации Kerberos/GSSAPI в сборке нет. Всё
остальное на месте, потому что идёт через OpenSSL, а не через Cyrus:

| Механизм | Работает |
|---|---|
| TLS / SSL | да |
| SASL PLAIN | да |
| SASL SCRAM-SHA-256 / SCRAM-SHA-512 | да |
| SASL OAUTHBEARER (в т. ч. OIDC через curl) | да |
| SASL GSSAPI / Kerberos | **нет** |

Сама librdkafka при попытке включить GSSAPI скажет об этом прямым текстом:
`No provider for SASL mechanism %s: recompile librdkafka with libsasl2 or openssl
support. Current build options: PLAIN SASL_SCRAM OAUTHBEARER`.

Таблица выше — про Linux. На Windows с Kerberos иначе: Cyrus SASL там не нужен вовсе.
Для механизма `GSSAPI` librdkafka под `_WIN32` берёт провайдер `Win32 SSPI`
(`src/rdkafka_sasl.c`, реализация — `src/rdkafka_sasl_win32.c`), то есть встроенный в
Windows SSPI, и этот файл компилируется в любой Windows-сборке. Собранный DLL
подтверждает: в его `builtin.features` есть `sasl_gssapi`
([§6.6](#66-проверки-готового-dll-на-windows)). В строке `BUILT_WITH` этого не видно — она
перечисляет только флаги из `win32_config.h`.

SSPI всегда работает от имени учётной записи, под которой запущен процесс, — для
сервера 1С это учётная запись службы. `sasl.kerberos.principal` на Windows игнорируется,
а `sasl.kerberos.keytab`, `sasl.kerberos.kinit.cmd` и
`sasl.kerberos.min.time.before.relogin` librdkafka отвергает ошибкой
`Configuration property "…" not supported in this build: Kerberos keytabs are not supported on Windows…`,
так что Linux-настройки с keytab на Windows не переносятся. Аутентификация против
настоящего KDC в проверке не участвовала.

Если Kerberos всё-таки понадобится — собирать librdkafka придётся без
`--source-deps-only`, с системным `libsasl2-dev`, и тогда у `.so` появится внешняя
зависимость от `libsasl2.so`, которую надо будет ставить на каждый сервер 1С.

### 8.3 `mkl_patch: libcurl: failed to apply patch`

**Симптом.** Первый запуск ступени 1 упал (по любой причине из §8.1 / §8.2), вы
исправили причину, запустили снова — и теперь падает раньше и на другом:

```
mkl_patch: libcurl: failed to apply patch …/mklove/modules/patches/libcurl.0000-no-runtime-linking-check.patch: see source dep build log for details
```

**Причина.** mklove патчит исходники curl, чтобы выключить в его `configure`
проверку «собрать и запустить программу со всеми библиотеками» — эта проверка
не проходит, потому что библиотеки лежат в `DESTDIR` вне путей `ld.so`. Патч
накладывается **безусловно, при каждом заходе** в установщик curl
(`mkl_patch libcurl 0000` в `mklove/modules/configure.libcurl`), а вот скачивание
тарболла пропускается, если `Makefile` уже есть. Итог: дерево от прошлого запуска
уже пропатчено, `patch -p1` видит это и возвращает ненулевой код, mklove считает
это ошибкой.

**Что делать.** Снести дерево зависимостей и начать ступень 1 с чистого листа:

```bash
# на нормальной ФС
rm -rf .build/librdkafka-v2.15.1/mklove/deps

# на fuseblk/NTFS — там, куда скрипт увёл сборку
rm -rf "${TMPDIR:-/tmp}/onec-librdkafka-build/librdkafka-v2.15.1/mklove/deps"
```

Если сомневаетесь, где именно рабочий каталог, — удалите его целиком, скрипт
переклонирует librdkafka заново. Это дольше, но надёжнее: `mklove/deps/src`
содержит `libcrypto`, `libcurl`, `libzstd`, `zlib`, и полуприменённое состояние
любого из них воспроизводит ту же ошибку.

### 8.4 Ошибки компоновки после смены версии librdkafka

**Симптом.** Обновили librdkafka, компонента не линкуется: неразрешённые символы
`rd_kafka_*` или `RdKafka::*`, либо, наоборот, дубли.

**Причина, вариант первый — устаревшее сборочное дерево.** В `build/` остался
CMake-кэш, помнящий старые пути и старые объектные файлы. `cmake --build` при смене
внешнего `.a` не пересобирает объекты, потому что их исходники не менялись.

```bash
scripts/build-component-linux.sh
```

`scripts/build-component-linux.sh` и `scripts\build-component-windows.ps1` сносят
сборочное дерево сами, перед каждой конфигурацией — это единственная причина, по
которой стоит предпочитать их ручному `cmake --build` в уже существующем каталоге.

**Причина, вариант второй — заголовки и библиотеки из разных сборок.** `src/rdkafka.h`
обновили, а `lib/linux64/*.a` (или `lib\win64\*.lib`) остались от прошлой версии.
На Windows это опаснее всего: часть таких сочетаний линкуется молча и портит память
в рантайме — см. [§4.4](#44-почему-lib-не-лежат-в-репозитории).

Сверить, что версия в заголовке соответствует ожидаемой:

```bash
grep -m1 'define RD_KAFKA_VERSION ' src/rdkafka.h
```

```
#define RD_KAFKA_VERSION 0x020f01ff
```

`0x02 0f 01` — это 2.15.1. И то же самое со стороны собранного бинарника:

```bash
nm -D out64/librdkafka_onec.so | grep rd_kafka_version
```

```
0000000000348230 T rd_kafka_version
0000000000348240 T rd_kafka_version_str
```

Эти две строки говорят только, что версию есть у кого спросить.

Расхождение лечится единственным способом — перезапуском ступени 1 целиком, чтобы
`.a`/`.lib` и `.h` снова приехали из одной сборки. На Linux смоук-тест проверяет версию
двумя независимыми способами (`rd_kafka_version_str()` и `rd_kafka_version()`),
поэтому такое расхождение он ловит сразу. На Windows этой страховки нет: `tests/` там
не собирается, а DLL не экспортирует `rd_kafka_version*`, так что и перенесённый
смоук-тест эту проверку пропускает ([§6.6](#66-проверки-готового-dll-на-windows)).

### 8.5 `curl operation failed with error code 35` при vcpkg install

**Симптом.** Windows, ступень 1, где-то на скачивании:

```
error: curl operation failed with error code 35 (SSL connect error).
error: Not a transient network error, won't retry download from https://github.com/madler/zlib/archive/v1.3.2.tar.gz
```

Повторный запуск падает на другом файле или проходит.

**Причина.** Обрывается TLS-рукопожатие при скачивании. На проверенной сборке весь
трафик vcpkg шёл через локальный HTTP-прокси (в логе —
`Using %HTTP(S)_PROXY% in environment variables`), и обрывы случались время от времени на разных адресах —
`github.com`, `www.nasm.us`. Тот же код бывает и без прокси, если TLS-трафик по дороге
фильтруется или перехватывается. vcpkg считает код 35 не временной ошибкой и попытку
не повторяет, так что одного оборванного соединения хватает, чтобы остановить весь
`vcpkg install`. Для
части инструментов у vcpkg есть запасные адреса, и туда он переходит сам (так было с
NASM: `Trying https://vcpkg.github.io/assets/nasm/…`). Для исходников портов их нет.

**Что делать.** Сначала скачать исходники отдельной командой и повторять её, пока она
не пройдёт без `error code`. Скачанное vcpkg кэширует в `.build\vcpkg\downloads`, и
ступень 1 потом берёт его оттуда:

```bat
set HTTPS_PROXY=http://<прокси>:<порт>
set HTTP_PROXY=http://<прокси>:<порт>
.build\vcpkg\vcpkg.exe install zlib:x64-windows-static zstd:x64-windows-static openssl:x64-windows-static "curl[core,ssl]:x64-windows-static" --recurse --only-downloads
```

Для обычного HTTP-прокси (таковы, например, локальные v2ray и shadowsocks) адрес пишется
со схемой `http://`, даже если через него ходят на `https://`-адреса: схема описывает
соединение с самим прокси, и `https://` означал бы TLS до прокси. Об этом же напоминает
подсказка, которую vcpkg выводит при ошибке скачивания. `git` берёт прокси из тех же
переменных, поэтому задавайте их и перед самой ступенью 1. Strawberry Perl, NASM и jom для OpenSSL этой командой заранее не
скачиваются — они приезжают уже во время сборки.

Если `github.com` напрямую недоступен, а прокси не задан, первым падает клон vcpkg —
`Failed to connect to github.com port 443`, — и скрипт на этом останавливается.

### 8.6 Ступень 1 на Windows падает на старой версии скрипта

До исправлений `scripts\build-librdkafka-windows.ps1` ни разу не доходил до
`lib\win64`. Если в логе одно из сообщений ниже — скрипт старый: обновите репозиторий и
запустите ступень 1 заново. Остатки старого запуска новый скрипт убирает сам.

| Сообщение | Причина |
|---|---|
| `The string is missing the terminator: ".` (в русском интерфейсе PowerShell — `В строке отсутствует завершающий символ: ".`) — сразу при запуске | скрипт был в UTF-8 без BOM с длинным тире внутри строки. Windows PowerShell 5.1 читает такой файл в ANSI-кодировке системы (на русской Windows — cp1251, на западной — cp1252), а в обеих байт `0x94` из тире — закрывающая кавычка `”` |
| `bootstrap-vcpkg.bat` не распознано как имя командлета | клон vcpkg не удался (чаще всего — сеть, [§8.5](#85-curl-operation-failed-with-error-code-35-при-vcpkg-install)), а скрипт код возврата `git clone` не проверял |
| `Invalid triplet name. Triplet names are all lowercase alphanumeric+hyphens.` / `on expression: $Triplet` | аргумент `-DVCPKG_TARGET_TRIPLET=$Triplet` без кавычек, [§4.2](#42-ступень-1--librdkafka-и-её-зависимости) |

Ещё одно сообщение старый скрипт сам не выдаёт:
`vcpkg manifest mode was disabled for a build directory where it was initially enabled.`
Старый скрипт не передаёт `-DVCPKG_MANIFEST_MODE=OFF`, и его дерево всегда остаётся в
manifest-режиме. Сообщение появляется, когда поверх `build-<arch>`, оставшегося от
старого скрипта, конфигурируют с флагами из
[§4.2](#42-ступень-1--librdkafka-и-её-зависимости) — руками или вручную поправленным
скриптом: переключить уже сконфигурированное дерево из manifest-режима в классический
vcpkg не даёт. Новый скрипт удаляет `build-<arch>` перед каждой конфигурацией; при ручной
сборке удалите его сами.

---

## 9. Ручки сборки

### 9.1 Компонента

| Ручка | Значение по умолчанию | Что делает |
|---|---|---|
| `CMAKE_BUILD_TYPE` | скрипты задают `Release` | Тип сборки для одноконфигурационных генераторов (Makefiles, Ninja). Для отладки: `cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo ..` |
| `--config <тип>` | `Release` в скрипте Windows | На Windows тип конфигурации задаётся **здесь**, а не через `CMAKE_BUILD_TYPE`: генератор Visual Studio многоконфигурационный |
| `RDKAFKA_WIN_EXTRA_LIBS` | пусто | Только Windows. Дополнительные имена `.lib` в `lib\win64` / `lib\win32`, которые разрешено линковать, — на случай, если vcpkg опять переименует зависимость. Только имена, без путей, через `;`: `"-DRDKAFKA_WIN_EXTRA_LIBS=ssl.lib;crypto.lib"`. `scripts\build-component-windows.ps1` этот параметр не пробрасывает — конфигурировать CMake придётся руками |

Больше опций у компоненты нет: список исходников, `LIBRDKAFKA_STATICLIB`, разрядность
и набор системных библиотек в `CMakeLists.txt` зафиксированы.

### 9.2 Тесты

Проект `tests/` — самостоятельный, корневой `CMakeLists.txt` его не подключает.
Все переменные — кэш-переменные CMake, то есть задаются через `-D…` при
конфигурации:

| Переменная | По умолчанию | Что делает |
|---|---|---|
| `ONEC_KAFKA_SO` | `<репозиторий>/out64/librdkafka_onec.so` | Какую компоненту грузят тесты. Отсутствующий файл — предупреждение, а не ошибка конфигурации: тесты грузят `.so` в рантайме |
| `ONEC_TESTS_SANITIZE` | `none` | `none` \| `address` \| `thread` \| `undefined`; `ON` — синоним `address` (и включает UBSan вместе с ASan) |
| `ONEC_TESTS_SOAK` | `OFF` | Включает soak-тест. Он не попадает в обычный прогон: выбирается `ctest -L soak` |
| `ONEC_TESTS_REQUIRE_BROKER` | `OFF` | Недоступный брокер становится **провалом**, а не пропуском. Нужен, когда зелёный `ctest` обязан означать «TLS действительно проверялся» |
| `ONEC_TESTS_SOAK_TIMEOUT` | `7200` | Таймаут ctest для soak-теста, секунды |

```bash
cmake -S tests -B tests/build -DONEC_TESTS_SOAK=ON -DONEC_TESTS_REQUIRE_BROKER=ON
```

Конфигурация печатает итоговый набор — это самый быстрый способ убедиться, что ручка
подхватилась:

```
-- onec-librdkafka tests
--   build type     : RelWithDebInfo
--   component      : …/out64/librdkafka_onec.so
--   sanitizer      : none
--   broker         : REQUIRED - an unreachable broker fails the run
--   soak test      : enabled, select it with 'ctest -L soak' (timeout 7200s)
```

Переменные окружения (не кэш-переменные):

| Переменная | Где смотрит | Смысл |
|---|---|---|
| `ONEC_KAFKA_SO` | сами тесты, `run-tests.sh` | Приоритет в тестах: `argv[1]` → эта переменная → значение, вкомпилированное cmake |
| `ONEC_TESTS_BUILD_DIR` | `run-tests.sh` | Сборочное дерево, по умолчанию `tests/build` |
| `TMPDIR` | `scripts/build-librdkafka-linux.sh` | Куда уходит сборка librdkafka с разделов без POSIX-прав |
| `ONEC_LINT_DIR` | `scripts/lint.sh` | Рабочий каталог статанализа |

Остальные `KAFKA_*` — параметры брокера и TLS-теста, они перечислены в
[tests/README.md](tests/README.md).
