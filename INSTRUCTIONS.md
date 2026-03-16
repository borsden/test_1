# Инструкции по проекту

Эта памятка описывает шаги и допущения, по которым можно воспроизвести локальную
сборку и работу тулкита для офлайн-разбора B3 UMDF (MBO flavor).

## 1. Предварительные требования

- Python 3.12+ с `pip` — используем для вспомогательных CLI (`click`) и
  библиотек `pyarrow`, `dpkt`.
- CMake ≥ 3.23, Ninja (или другой поддерживаемый генератор) и компилятор с
  поддержкой C++20.
- Локальный JDK (`third_party/jdk-21.0.2+13`) — гоняем через него `sbe-all` при
  генерации кодеков.

Минимальный набор Python-зависимостей можно поставить так:

```bash
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

В `requirements.txt` лежат минимальные зависимости для CLI (`click`, `dpkt`). При
необходимости можно добавить тестовые зависимости (pytest и т.д.).

## 2. Сторонние исходники

```
third_party/
  jdk-21.0.2+13/                 # локальный JDK для codegen'а
  sbe-all-1.38.0.jar             # готовый jar с генератором SBE
```

Jar можно взять из официального Maven-репозитория Real Logic SBE:
<https://repo1.maven.org/maven2/uk/co/real-logic/sbe-all/1.38.0/sbe-all-1.38.0.jar>
(или нужной версии) и сохранить в `third_party/sbe-all-1.38.0.jar`. Скрипт
`scripts/generate_codecs.py` позволяет явно указать путь к jar, схеме и JDK.

## 3. Генерация SBE-кодеков

```
python scripts/generate_codecs.py \
    --schema schema/b3-market-data-messages-1.8.0.xml \
    --jar third_party/sbe-all-1.38.0.jar \
    --output cpp_decoder/sbe-generated
```

Ключевые опции CLI:

- `--java-bin` / `--java-opts` — выбор конкретного JDK и JVM-флагов;
- `--define -Dsbe.foo=bar` — дополнительные `-D` свойства для генератора;
- `--target-language` и `--no-stubs` — управление типом выходного кода;
- `--dry-run` — показать итоговую команду без запуска.

По умолчанию берутся схема из `schema/`, jar из `third_party/` и выходной
каталог `cpp_decoder/sbe-generated`. Для смены версии достаточно передать иные
пути в CLI.

## 4. Сборка C++

```
cmake -S . -B build -G Ninja
cmake --build build
```

После сборки бинарь `b3sbe_decode` лежит в каталоге `build/`.

## 5. Запуск

Рекомендуемый путь — helper-скрипт `scripts/decode_pcap_to_arrow.py`, который
конфигурирует/собирает проект и запускает бинарь:

```
python scripts/decode_pcap_to_arrow.py \
    --input /mnt/Projects/test_1/data/20241118 \
    --output ./parsed \
    --max-packets 10000   # опционально
```

Для прямого запуска без скрипта используйте `./build/b3sbe_decode` с теми же
флагами (`--input`, `--output`, `--tables`, `--max-packets`, `--no-validate`).

Важные опции helper-скрипта:

- `--tables instruments,snapshot_orders,...` — ограничить список выводимых
  таблиц (по умолчанию пишем все поддерживаемые).
- `--max-packets N` — обрезать каждый PCAP после N пакетов (удобно для быстрых
  тестов).
- `--no-validate` — отключить проверку `schemaId/schemaVersion` в заголовках.
- `--skip-build` — пропустить `cmake -S/-B` и `cmake --build`, если проект уже
  собран.
- `--build-dir` — указать альтернативный каталог для артефактов CMake.

Результатом выполнения станут Parquet-файлы в указанном `--output` каталоге:
`instruments.parquet`, `snapshot_orders.parquet`, `incremental_*`, `errors.parquet`
и т.д. EmptyBook/ChannelReset пишутся в отдельные таблицы
`incremental_empty_books.parquet` и `incremental_channel_resets.parquet`. Каждая
таблица — потоково записанная версия соответствующей структуры, описанной в
`batch_builder.h`.

## 6. Структура исходников

- Нативный код/писатели: `cpp_decoder/*.cpp`, сгенерированные заголовки —
  `cpp_decoder/sbe-generated/**`.
- `src_old/` — архив прежней Python-версии, оставлен для справок.
- `scripts/` — `generate_codecs.py` (SBE codegen) и `decode_pcap_to_arrow.py`
  (build+run helper).
- Табличные писатели (`parquet_writer.*`) обязаны стримить данные в Parquet
  сразу после декодирования, без накопления всего архива в памяти.

На текущем этапе Python-слой не подключается к нативному декодеру напрямую — он
работает только с готовыми Parquet-таблицами. Когда replay/аналитика будут
возвращены, они будут читать результаты из `--output` каталога.

## 7. Постобработка (flatten) Parquet-таблиц

После работы `b3sbe_decode` получаем «широкие» файлы вида
`parsed/incremental_orders.parquet`, `snapshot_orders.parquet` и т.д. Для удобной
работы с отдельными инструментами/каналами есть вспомогательный CLI
`scripts/flatten_parquet.py`. Он стримово читает каждую таблицу и раскладывает её
в Hive-партиционированные директории внутри `parsed_flatten/<table>/`:

```
python scripts/flatten_parquet.py \
    --parsed-dir data/parsed \
    --output-dir data/parsed_flatten \
    --tables incremental_orders,incremental_deletes \
    --overwrite \
    --batch-size 2000000
```

Ключевые моменты:

- `--tables` — можно перечислить подмножество таблиц; `*` (по умолчанию)
  прогоняет все (`instruments`, `snapshot_*`, `incremental_*`,
  `incremental_other`).
- `--overwrite` удаляет только каталоги перечисленных таблиц внутри
  `--output-dir`, остальные результаты остаются.
- `--batch-size` управляет размером Arrow-батча при стриминге больших таблиц
  (по умолчанию 2 млн строк; можно уменьшить, если не хватает памяти).
- Каждая таблица записывается в `output_dir/<table>/channel_hint=…/security_id=…
  [/feed_leg=…]/part-*.parquet`. Например, `data/parsed_flatten/
  incremental_orders/channel_hint=78/security_id=100000098498/feed_leg=A/…`.
- Запись идёт вручную, без финального «тихого» этапа, поэтому выполнение
  завершается сразу после завершения прогресс-бара.

Скрипт не изменяет исходные Parquet-файлы из `--parsed-dir` и при необходимости
может быть запущен повторно на любом подмножестве таблиц.
