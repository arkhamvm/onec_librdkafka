# Тестовый брокер Kafka 4.x с TLS

Поднимает в Docker одноузловой брокер Apache Kafka 4.x (KRaft, без ZooKeeper)
с двумя слушателями: PLAINTEXT для отладки и SSL для собственно теста.

Смысл этого каталога — доказать, что компонент, пересобранный на librdkafka
2.15.1, действительно умеет TLS против брокера Kafka 4.x, чего librdkafka 2.3.0
не умела.

## Быстрый старт

```bash
cd tests/docker
./up.sh          # сгенерирует сертификаты (если надо), поднимет брокер, дождётся готовности
./down.sh        # остановит и удалит контейнеры вместе с томом данных
```

`up.sh` в конце печатает всё, что нужно тесту:

```
[up] Kafka 4.3.1 test broker is up.

  image                apache/kafka:4.3.1
  reported version     4.3.1   (asserted: major == 4)
  PLAINTEXT bootstrap  localhost:9092     (debugging only)
  SSL bootstrap        localhost:9093     <-- use this one
  test topic           onec-librdkafka-ssl-test

  librdkafka / component configuration:
    security.protocol = ssl
    ssl.ca.location   = .../tests/docker/secrets/ca.pem

  negative control (must NOT verify the broker):
    ssl.ca.location   = .../tests/docker/secrets/other-ca.pem

  KAFKA_TEST_BROKER_VERSION=4.3.1
```

В первой строке — **та версия, которую брокер сообщил о себе сам**, а не тег
образа из `.env` (см. «Проверка версии брокера» ниже).

## Что именно поднимается

| Параметр            | Значение                                        |
|---------------------|-------------------------------------------------|
| Образ               | `apache/kafka:4.3.1` (последний GA-релиз 4.x)   |
| Режим               | KRaft, один узел, роли `broker,controller`      |
| PLAINTEXT           | `localhost:9092`                                |
| SSL                 | `localhost:9093`                                |
| CONTROLLER          | `0.0.0.0:9094`, наружу не публикуется           |
| Проверка клиента    | `ssl.client.auth=none` — только серверный TLS   |
| Протоколы TLS       | TLSv1.2, TLSv1.3                                |
| Автосоздание топиков| включено                                        |
| Тестовый топик      | `onec-librdkafka-ssl-test`, создаётся init-шагом|

Имя топика в `.env` (`KAFKA_TEST_TOPIC`) обязано совпадать с тем, которое по
умолчанию берёт `tests/kafka_ssl_test.cpp`. Пока они различались, init-шаг
создавал один топик, а тест работал с другим — и всё проходило только потому,
что у брокера включено автосоздание топиков, то есть init-шаг ничего не
доказывал. `run-tests.sh` читает значение из `.env` и передаёт его тесту.

Порты публикуются только на `127.0.0.1`. Номер порта на хосте совпадает с
портом в контейнере — это важно, потому что `advertised.listeners` отдаёт
клиенту именно эти номера, а тест выполняется на хосте, а не в контейнере.

mTLS сознательно не включён: проверяем серверный TLS, клиенту достаточно
`ca.pem`.

## Файлы

| Файл                 | Назначение                                                        |
|----------------------|-------------------------------------------------------------------|
| `.env`               | единственное место, где заданы версия образа, порты и пароль      |
| `gen-certs.sh`       | генерация CA, сертификата брокера, JKS-хранилищ и PEM-бандла      |
| `compose.yml`        | сам брокер и одноразовый init-шаг создания топика                 |
| `up.sh` / `down.sh`  | запуск с ожиданием готовности / остановка                         |
| `_common.sh`         | общие функции, поиск рабочих `openssl` и `keytool`                |
| `secrets/`           | результат работы `gen-certs.sh`, в git не попадает                |
| `secrets/broker-version` | версия, которую сообщил запущенный брокер; пишет `up.sh`      |

## Сертификаты

`gen-certs.sh` кладёт в `secrets/`:

* `ca.pem` — самоподписанный тестовый CA в формате PEM. **Это то, что нужно
  librdkafka**: она читает `ssl.ca.location` (или `ssl.ca.pem`), JKS не умеет;
* `ca.key` — ключ этого CA;
* `other-ca.pem` / `other-ca.key` — **второй, ни с чем не связанный CA**. Он
  ничего не подписывает и брокеру неизвестен. Он нужен как отрицательный
  контроль проверки сертификата: тест, который подсовывает librdkafka этот
  бандл, обязан получить отказ. Без него пришлось бы указывать на системный
  список CA, который, во-первых, есть не везде, а во-вторых, не гарантирует
  отказ. `gen-certs.sh` заодно проверяет, что `broker.pem` этим CA **не**
  проверяется — иначе отрицательный контроль перестал бы быть отрицательным;
* `broker.pem` / `broker.key` — сертификат брокера, `CN=localhost`,
  `SAN = DNS:localhost, DNS:kafka, IP:127.0.0.1`;
* `kafka.keystore.jks`, `kafka.truststore.jks` — хранилища для брокера;
* `keystore_creds`, `key_creds`, `truststore_creds` — файлы с паролем; штатный
  entrypoint образа `apache/kafka` читает пароли именно из файлов, а не из
  переменных окружения;
* `client-ssl.properties` — конфиг для JVM-утилит `kafka-*.sh`.

Пароль задаётся один раз в `.env` (`KAFKA_CERT_PASSWORD`) и оттуда попадает и в
хранилища, и в `compose.yml`.

Скрипт идемпотентен. При повторном запуске он проверяет, что всё на месте, что
`broker.pem` подписан текущим `ca.pem`, что `other-ca.pem` его, наоборот, не
подписывает, что сроки не истекли, что SAN покрывает `localhost` и `127.0.0.1`
и что хранилища открываются текущим паролем. Если
что-то не сходится — перегенерирует **всё** во временный каталог и подменяет его
целиком, поэтому рассогласованной пары «CA + сертификат» не получится даже при
прерванном запуске.

```bash
./gen-certs.sh           # перегенерировать только при необходимости
./gen-certs.sh --force   # перегенерировать принудительно
```

### openssl и keytool

`_common.sh` сам находит рабочие утилиты и при необходимости запускает их
внутри образа Kafka через `docker run --rm`, так что локальный JDK не нужен.

Отдельно про openssl на машинах, где собирали librdkafka: `scripts/build-*.sh`
ставит свой OpenSSL в `/usr/local/ssl`, и `/usr/bin/openssl` (сборка Ubuntu
3.0.13) начинает подгружать чужую `libcrypto.so.3` 3.3.1. На таком openssl любой
вызов с `-addext` **пишет корректный сертификат и затем падает с SIGSEGV**.
Поэтому `_common.sh` не просто ищет бинарник, а сверяет версию сборки с версией
загруженной библиотеки и прогоняет пробный вызов, проверяя код возврата.

Переменные окружения:

```bash
ONEC_OPENSSL=/path/to/openssl     # попробовать сначала этот бинарник
ONEC_FORCE_DOCKER_TOOLS=1         # не трогать хост, всё делать в образе Kafka
```

## Проверка готовности

Healthcheck контейнера проверяет две вещи сразу:

1. брокер отвечает на настоящий запрос Kafka API по PLAINTEXT;
2. на порту SSL выполняется TLS-рукопожатие с проверкой цепочки и имени хоста.

Поэтому `up.sh` ждёт статуса `healthy`, а не спит фиксированное время. Если
брокер не поднялся за `READY_TIMEOUT` (по умолчанию 180 с), `up.sh` печатает
хвост лога брокера и завершается с ненулевым кодом.

`up.sh` дополнительно проверяет, что запущенный брокер отдаёт сертификат,
проверяемый текущим `secrets/ca.pem`. Если сертификаты перегенерировали под уже
работающим брокером, он пересоздаст контейнер — брокер читает keystore только
при старте.

## Проверка версии брокера

Тег образа в `.env` — это пожелание, а не факт: тег можно поменять, локальный
образ может оказаться старым, апстрим может передвинуть тег. Раньше `up.sh`
безусловно печатал «Kafka 4.x test broker is up», и с `KAFKA_IMAGE_TAG=3.9.1`
поднимался брокер 3.x, а весь набор тестов всё так же был зелёным — то есть
доказывал не то, ради чего написан.

Теперь после ожидания `healthy` `up.sh` спрашивает у самого контейнера:

```bash
docker exec onec-librdkafka-test-kafka /opt/kafka/bin/kafka-topics.sh --version
```

и сравнивает мажорную версию с `KAFKA_TEST_EXPECT_BROKER_MAJOR` (по умолчанию
`4`). Не совпало — `up.sh` завершается с ненулевым кодом и печатает, что именно
он получил:

```
[up] ERROR: this broker is NOT Kafka 4.x
[up]   reported version : 3.9.1
[up]   required major   : 4
[up]   image tag in .env: apache/kafka:3.9.1
```

Совпало — версия попадает в баннер вместо прежней жёстко зашитой строки,
экспортируется как `KAFKA_TEST_BROKER_VERSION` и записывается в
`secrets/broker-version`. Оттуда её читает `run-tests.sh` (`up.sh` запускается
дочерним процессом, экспорт до родителя не доходит) и передаёт тесту вместе с
`KAFKA_TEST_EXPECT_BROKER_MAJOR`, так что версию проверяет ещё и сам тест.
`down.sh` этот файл удаляет: устаревшая версия хуже отсутствующей.

Проверить, что защита работает, можно не поднимая брокер 3.x:

```bash
KAFKA_TEST_EXPECT_BROKER_MAJOR=3 ./up.sh   # должно упасть на живом 4.3.1
```

## Ручная проверка TLS

```bash
openssl s_client -connect localhost:9093 \
    -CAfile tests/docker/secrets/ca.pem -brief </dev/null
```

Ожидаемый вывод содержит `Verification: OK`. На машине с испорченным
`/usr/bin/openssl` (см. выше) берите `/usr/local/ssl/bin/openssl`.

Обратная проверка — с «чужим» CA она обязана провалиться:

```bash
openssl s_client -connect localhost:9093 \
    -CAfile tests/docker/secrets/other-ca.pem -verify_return_error -brief </dev/null
```

Продюсер и консьюмер через SSL:

```bash
docker exec -i onec-librdkafka-test-kafka \
  /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9093 \
    --command-config /etc/kafka/secrets/client-ssl.properties \
    --topic onec-librdkafka-ssl-test

docker exec onec-librdkafka-test-kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9093 \
    --command-config /etc/kafka/secrets/client-ssl.properties \
    --topic onec-librdkafka-ssl-test --from-beginning --max-messages 3
```

## Прочие команды

```bash
./up.sh --recreate          # снести кластер и поднять с нуля
./down.sh --purge-certs     # остановить и удалить ещё и secrets/
docker compose -f compose.yml logs -f kafka
```

CI не предусмотрен — всё запускается руками.
