# Инструкции по проекту

Эта памятка описывает шаги и допущения, по которым можно воспроизвести локальную
сборку и работу тулкита для офлайн-разбора B3 UMDF (MBO flavor).

## 1. Предварительные требования

- Python 3.12+ с `pip` — используется и для CLI, и для самого orderbook-пайплайна.
  Базовые библиотеки: `click`, `pyarrow`, `pandas`, `tqdm`.
- CMake ≥ 3.23, Ninja (или другой поддерживаемый генератор) и компилятор с
  поддержкой C++20 для сборки нативного декодера.
- Локальный JDK (`third_party/jdk-21.0.2+13`) — нужен для запуска `sbe-all`
  при генерации кодеков.

Минимальный набор Python-зависимостей можно поставить так:

```bash
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

`requirements.txt` содержит минимальный набор Python-зависимостей (click,
pyarrow, pandas, tqdm и т.д.). При необходимости можно добавить тестовые
пакеты (pytest и др.).

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

Результат раскладывается по каналам и фидам:

```
parsed/
  channel_78/
    instruments.parquet
    snapshot_headers.parquet
    ...
    feed_A/
      incremental_orders.parquet
      incremental_deletes.parquet
      ...
    feed_B/
      ...
  channel_88/
    ...
```
Каждая таблица — потоково записанная версия структуры из `batch_builder.h`.

## 6. Структура исходников

- Нативный код/писатели: `cpp_decoder/*.cpp`, сгенерированные заголовки —
  `cpp_decoder/sbe-generated/**`. Декодер сразу стримит таблицы в Parquet,
  не накапливая весь PCAP в памяти.
- Python-пайплайн orderbook: каталог `orderbook/` содержит reusable-модули
  (реплей, инструменты, билдеры выходов), которые используют и CLI в `scripts/`,
  и любые ноутбуки/утилиты поверх Parquet-таблиц.
- `scripts/` — набор вспомогательных CLI (`generate_codecs.py`,
  `decode_pcap_to_arrow.py`, `convert_arrow_to_csv.py` и др.).

Нативный декодер остаётся самостоятельным шагом: он выдаёт структуру каталогов
`parsed/channel_XX/...`, а Python-скрипты потребляют эти файлы для дальнейшей
обработки.

## 7. Конвертация Parquet→CSV

Для быстрых проверок можно выгрузить любые таблицы в CSV:

```
python scripts/convert_arrow_to_csv.py \
    --input data/parsed \
    --output tmp/csv
```

Скрипт рекурсивно проходит по входной директории и конвертирует все Parquet-файлы,
сохраняя относительную структуру. Ключевые флаги:

- `--pattern instruments,snapshot_orders` — ограничить список файлов по имени;
- `--overwrite` — разрешить перезапись существующих CSV;
- `--max-rows 1000` — усечь каждую таблицу (удобно для снифпетов).

## 8. Построение L3/L2/L1 событий

После декодера основной шаг — `scripts/build_orderbook.py`. Скрипт читает
Parquet-таблицы из `--data-root`, прогоняет реплей orderbook и сразу стримит
снимки в `book_l3` (MBO/L3), `book_l2` (MBP/L2) и `book_l1` (BBO/L1) per тикер:

```
python scripts/build_orderbook.py \
    --data-root parsed \
    --output-root data/orderbook \
    --ticker WDOF25 --ticker WINZ25 \
    --feed feed_A --feed feed_B \
    --since 2024-11-18T10:00:00Z \
    --progress --csv
```

Ключевые флаги:

- `--ticker` — перечисление тикеров; для каждого создаётся папка
  `output_root/{ticker}/book_l{1,2,3}.(parquet|csv)`.
- `--feed feed_A --feed feed_B` — порядок приоритета ног; берём первую
  доступную ногу на канале.
- `--since` (ISO-8601 или наносекунды) — отсечение событий по времени (или
  реплеим всё после snapshot, если флаг не задан).
- `--csv` — писать CSV вместо Parquet.
- `--progress` — включить tqdm.
- `--no-warnings` — заглушить предупреждения пайплайна.
- `--no-rpt-warnings` — отключить предупреждения про разрывы rptSeq для тех
  шаблонов, которые пока не покрыты (orderbook-критичные шаблоны проверяются,
  но на второстепенных могут быть ложные срабатывания).

На выходе получаем три файла на тикер: `book_l3.*` (все ордера после каждого
EndOfEvent), `book_l2.*` (агрегированные уровни) и `book_l1.*` (best bid/offer
по каждому событию). Дополнительно CLI печатает итоги по количеству
событий/обновлений.
