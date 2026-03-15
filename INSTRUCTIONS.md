# Инструкции по проекту

Эта памятка описывает шаги и допущения, по которым можно воспроизвести локальную
сборку и работу тулкита для офлайн-разбора B3 UMDF (MBO flavor).

## 1. Предварительные требования

- Python 3.12+ c `pip` (нужен для установки `pyarrow`, чтобы получить
  `libarrow`/`libparquet`).
- CMake ≥ 3.23, Ninja (или другой поддерживаемый генератор) и компилятор с
  поддержкой C++20.
- Локальный JDK (`third_party/jdk-21.0.2+13`) — используем для запуска SBE
  codegen'а даже на машинах без системной Java.

## 2. Сторонние исходники

```
third_party/
  jdk-21.0.2+13/                 # локальный JDK для codegen'а
  sbe-all-1.38.0.jar             # готовый jar с генератором SBE
```

Jar можно взять из официального Maven-репозитория Real Logic SBE:
<https://repo1.maven.org/maven2/uk/co/real-logic/sbe-all/1.38.0/sbe-all-1.38.0.jar>
(или нужной версии) и сохранить в `third_party/sbe-all-1.38.0.jar`. При
необходимости обновления достаточно заменить этот файл и задать новую версию в
`scripts/generate_codecs.sh`.

## 3. Генерация SBE-кодеков

```
./scripts/generate_codecs.sh
```

По умолчанию скрипт использует:

- схему `schema/b3-market-data-messages-1.8.0.xml`;
- выходной каталог `cpp_decoder/sbe-generated/b3_umdf_mbo_sbe/*.h`;
- jar `third_party/sbe-all-1.38.0.jar`.

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
./scripts/decode_pcap_to_arrow.py \
    --input /mnt/Projects/test_1/data/20241118 \
    --output ./parsed \
    --max-packets 10000   # опционально
```

Для прямого запуска без скрипта используйте `./build/b3sbe_decode` с теми же
флагами (`--input`, `--output`, `--tables`, `--max-packets`, `--no-validate`).

## 6. Структура исходников

- Нативный код/писатели: `cpp_decoder/*.cpp`, сгенерированные заголовки —
  `cpp_decoder/sbe-generated/**`.
- `src_old/` — архив прежней Python-версии, оставлен для справок.
- `scripts/` — `generate_codecs.sh` (SBE codegen) и `decode_pcap_to_arrow.py`
  (build+run helper).
- Табличные писатели (`parquet_writer.*`) обязаны стримить данные в Parquet
  сразу после декодирования, без накопления всего архива в памяти.
