# Заметки

## Почему C++ вместо чистого Python

- **Генерация кодеков.** Официальный Real Logic SBE мгновенно выдаёт C++ flyweight'ы.
  Python-генератор из форка пока экспериментальный: теряет половину полей,
  игнорирует optional/NULL и repeating groups. Делать production-парсер поверх него —
  значит переписывать всё вручную.
- **Производительность и память.** C++-декодер читает PCAP потоково и может писать
  Parquet сразу, без хранения сотен тысяч сообщений. Python (struct.unpack, GIL,
  аллокация dict'ов) медленнее на порядок.
- **Зависимость от схемы.** C++ flyweight'ы приходят из официального XML. Любое
  обновление схемы прогоняем через `scripts/generate_codecs.sh`. Python нужно было бы
  поддерживать вручную, что ломает требование «использовать официальный toolchain».
- **Совместимость со схемой.** В `data/20241118/b3-market-data-messages-1.8.0.xml`
  прямо указано: *«Fully tested with Java codecs generated from sbe-tool 1.25.3»*.
  IR один и тот же для Java и C++ — значит, C++ flyweight'ы соответствуют тем же
  правилам, и мы можем уверенно на них опираться (используем официальный jar
  1.38.0-SNAPSHOT).
- **Интеграция с Parquet.** В C++ можно напрямую использовать Arrow writer и
  стримить батчи на диск; Python-слой всё равно пришлось бы реализовывать как
  C-расширение.

Вывод: C++ — основной слой парсинга, Python служит для оркестрации,
нормализации и ресёрча.

## Что лежит в `data/20241118`

По каждому каналу (префиксы `78_` и `88_`) биржа выкладывает четыре PCAP:

- `*_Instrument.pcap` — справочник инструментов.
- `*_Snapshot.pcap` — снапшоты книги.
- `*_Incremental_feedA.pcap`, `*_Incremental_feedB.pcap` — две ноги
  инкрементального потока. Это параллельные multicast'ы c идентичным содержимым,
  отличие только в сетевом адресе (A/B).

Каналы 78 и 88 — независимые multicast-потоки с разными группами инструментов.
FeedA и FeedB внутри канала должны быть эквивалентны (это дубли для
отказоустойчивости).

## Какие Parquet-таблицы пишем

Для каждого файла указываем источник и назначение, а также набор колонок.

### `instruments.parquet` — `SecurityDefinition_12`

Описание: справочник инструментов (одна строка на `security_id`).

Колонки:
- `source_file` — имя PCAP, откуда пришёл `SecurityDefinition`.
- `packet_sequence_number` — номер UDP-пакета внутри канала (от PacketHeader).
- `packet_sending_time_ns` — timestamp PacketHeader (ns UTC).
- `security_id` — уникальный ID инструмента.
- `symbol` — тикер.
- `security_exchange` — биржевой код площадки.
- `security_group` — группировка инструментов по UMDF.
- `security_type` / `security_type_raw` — тип (FUT, OPT, ETF...), строка + сырой enum.
- `security_sub_type` — числовой подтип (в UMDF задан как uint16).
- `cfi_code` — ISO CFI.
- `asset` — класс базового актива.
- `product` / `product_raw` — FIX Product.
- `put_or_call` / `_raw` — признак опциона.
- `exercise_style` / `_raw` — американский/европейский и т.п.
- `min_price_increment_mantissa`, `min_price_increment` — шаг цены (int + scaled double).
- `strike_price_mantissa`, `strike_price` — страйк для опционов.
- `contract_multiplier_mantissa`, `contract_multiplier` — множитель контракта.
- `price_divisor_mantissa`, `price_divisor` — делитель цены (quotations).
- `quantity_multiplier` — множитель количества.
- `trading_reference_price_mantissa`, `trading_reference_price` — референсная цена.
- `unit_of_measure`, `unit_of_measure_qty` — единица измерения и значение.
- `issue_date`, `maturity_date` — даты выпуска/погашения (LocalMktDate).
- `security_desc` — текстовое описание.
- `currency`, `base_currency`, `settle_currency` — валютные поля.
- `raw_template_id` — контроль (должен быть 12).

### `instrument_underlyings.parquet` — группа `noUnderlyings`

Описание: список базовых активов сложного инструмента.

Колонки: 
- `source_file` — PCAP-источник сообщения.
- `security_id` — родительский инструмент.
- `packet_sequence_number` — последовательность пакета.
- `underlying_security_id` — ID базового инструмента.
- `underlying_security_exchange` — биржа базового.
- `underlying_symbol` — тикер базового.
- `position_in_group` — порядок в repeating group (0..N-1).

### `instrument_legs.parquet` — группа `noLegs`

Описание: составные ноги стратегий/спредов.

Колонки:
- `source_file`, `security_id`, `packet_sequence_number` — lineage.
- `leg_security_id` — ID ноги.
- `leg_symbol` — тикер ноги.
- `leg_side_raw`, `leg_side` — сторона (BUY=1, SELL=2) и строковая форма.
- `leg_ratio_qty_mantissa`, `leg_ratio_qty` — коэффициент количества (int64, double).
- `position_in_group` — позиция в `noLegs`.

### `instrument_attributes.parquet` — группа `noInstrAttribs`

Описание: дополнительные атрибуты инструмента (канальные флаги, особенности
торговли и т.д.).

Колонки:
- `source_file`, `security_id`, `packet_sequence_number`.
- `attribute_type` — числовой код `InstrAttribType`.
- `attribute_value` — строковое значение (через enum или fallback `str(value)`).
- `position_in_group` — индекс в `noInstrAttribs`.

### `snapshot_headers.parquet` — `SnapshotFullRefresh_Header_30`

Описание: метаданные снапшота книги для `security_id` и канала.

Колонки:
- `source_file` — PCAP (обычно Snapshot feed).
- `channel_hint` — канал из имени файла (78/88).
- `packet_sequence_number`, `packet_sending_time_ns` — PacketHeader.
- `security_id` — инструмент снапшота.
- `last_msg_seq_num_processed` — последний rptSeq применённый на бирже.
- `tot_num_bids`, `tot_num_offers` — количество заявок в снапшоте.
- `last_rpt_seq` — rptSeq последнего snapshot entry.
- `snapshot_header_row_id` — уникальный surrogate-ключ.

### `snapshot_orders.parquet` — `SnapshotFullRefresh_Orders_MBO_71`

Описание: сами заявки из снапшота (multilevel order book).

Колонки:
- `source_file`, `channel_hint`, `packet_sequence_number`, `packet_sending_time_ns`.
- `snapshot_header_row_id` — связь с заголовком.
- `security_id`, `security_exchange` — инструмент.
- `side`, `side_raw` — сторона заявки.
- `price_mantissa`, `price` — цена.
- `size` — объём.
- `position_no` — позиция заявки (Tag 29043).
- `entering_firm` — код участника.
- `md_insert_timestamp_ns` — время вставки.
- `secondary_order_id` — идентификатор заявки (Tag 198).
- `row_in_group` — номер записи внутри `noMDEntries`.

### `incremental_orders.parquet` — `Order_MBO_50`

Описание: добавление/изменение заявки в инкрементальном потоке.

Колонки:
- `source_file`, `channel_hint`, `feed_leg` — откуда пришло событие.
- `packet_sequence_number`, `packet_sending_time_ns`, `message_index_in_packet` —
  пакетная позиция.
- `security_id` — инструмент.
- `match_event_indicator_raw` — битовое поле (EOL, recovery, etc.).
- `md_update_action_raw`, `md_update_action` — действие (NEW/CHANGE/DELETE_THRU...).
- `md_entry_type_raw`, `side` — тип записи и сторона (bid/offer).
- `price_mantissa`, `price` — цена.
- `size` — количество.
- `position_no` — идентификатор заявки внутри книги.
- `entering_firm` — фирма поставившая ордер.
- `md_insert_timestamp_ns` — биржевое время вставки.
- `secondary_order_id` — Tag 198 (стабильный ID).
- `rpt_seq` — счётчик событий per security.
- `md_entry_timestamp_ns` — метка из Incremental (offset 8).

### `incremental_deletes.parquet` — `DeleteOrder_MBO_51`

Описание: удаление конкретной заявки из инкрементального потока.

Колонки:
- `source_file`, `channel_hint`, `feed_leg`, `packet_sequence_number`,
  `packet_sending_time_ns`.
- `security_id` — инструмент.
- `match_event_indicator_raw` — флаги события.
- `md_update_action_raw`, `md_update_action` — действие (`delete`, `delete_thru`).
- `md_entry_type_raw`, `side` — тип записи/сторона.
- `position_no` — позиция удаляемой заявки.
- `size` — присланный биржей объём (может быть null).
- `secondary_order_id` — Tag 198 идентификатор.
- `md_entry_timestamp_ns` — временная метка сообщения.
- `rpt_seq` — счётчик событий per security.

### `incremental_mass_deletes.parquet` — `MassDeleteOrders_MBO_52`

Описание: удаления диапазона позиций (wipe стеков).

Колонки:
- `source_file`, `channel_hint`, `feed_leg`, `packet_sequence_number`,
  `packet_sending_time_ns`.
- `security_id` — инструмент.
- `match_event_indicator_raw` — флаги события.
- `md_update_action_raw`, `md_update_action` — действие (обычно `delete_from`).
- `md_entry_type_raw`, `side` — тип записи/сторона.
- `start_position`, `end_position` — диапазон позиций к удалению.
- `rpt_seq` — счётчик событий.
- `md_entry_timestamp_ns` — временная метка сообщения.

### `incremental_trades.parquet` — `Trade_53`

Описание: события сделок (Volume prints).

Колонки:
- `source_file`, `channel_hint`, `feed_leg`, `packet_sequence_number`,
  `packet_sending_time_ns`.
- `security_id` — инструмент сделки.
- `match_event_indicator_raw` — флаги события.
- `trading_session_id` — идентификатор торговой сессии.
- `trade_condition_raw` — битовые условия (auction, after-hours...).
- `trd_sub_type_raw` — подтип (RFQ, sweep, cross ...).
- `price_mantissa`, `price` — цена сделки.
- `size` — объём сделки.
- `trade_id` — Tag 1003.
- `buyer_firm`, `seller_firm` — идентификаторы участников.
- `trade_date` — календарная дата сделки.
- `md_entry_timestamp_ns` — временная метка сообщения.
- `rpt_seq` — последовательность per security.

### `incremental_empty_books.parquet` — `EmptyBook_9`

Описание: события «книга инструмента очищена». После EmptyBook биржа шлёт
восстановительные `Order_MBO` (с флагом recovery в `matchEventIndicator`), а
`rptSeq` по инструменту начинается заново с 1. Эти строки помогают Python
replay-сервису сбрасывать локальную книгу точечно.

Колонки:
- `source_file`, `channel_hint`, `feed_leg`, `packet_sequence_number`,
  `packet_sending_time_ns`.
- `security_id` — какая книга очищена.
- `match_event_indicator_raw` — биты события (5-й бит = recovery message).
- `md_entry_timestamp_ns` — временная метка EmptyBook.

### `incremental_channel_resets.parquet` — `ChannelReset_11`

Описание: сброс всего канала/ноги. После ChannelReset клиент обязан очистить
все книги по этому каналу, дождаться `EmptyBook` для нужных инструментов и
применить новый recovery-поток. Snapshot feed для канала тоже переинициализируется.

Колонки:
- `source_file`, `channel_hint`, `feed_leg`, `packet_sequence_number`,
  `packet_sending_time_ns`.
- `match_event_indicator_raw` — биты события (показывают recovery).
- `md_entry_timestamp_ns` — метка времени.

### `incremental_other.parquet` — прочие инкрементальные шаблоны

Описание: все templateId, для которых ещё нет typed-таблицы (SecurityStatus,
PriceBand, Sequence, News и т.д.). EmptyBook и ChannelReset теперь живут в
самостоятельных таблицах, здесь остаются остальные редкие шаблоны.

Колонки:
- `source_file` — исходный PCAP.
- `channel_hint`, `feed_leg` — канал и нога, извлечённые из имени файла/PacketHeader.
- `template_id`, `template_name` — идентификатор и человекочитаемое имя темплейта.
- `security_id` — инструмент (для ChannelReset = 0, событие относится ко всему каналу).
- `rpt_seq` — rptSeq, если сообщение его содержит (для EmptyBook/ChannelReset сервер не
  присылает поле, поэтому тут 0).
- `match_event_indicator_raw` — биты matchEventIndicator (важно для recovery flag).
- `md_entry_timestamp_ns` — временная метка события.
- `packet_sequence_number`, `packet_sending_time_ns` — PacketHeader.
- `body_hex` — hex-дамп тела SBE.
- `decode_status` — `ok` для сообщений, которые мы декодировали (EmptyBook,
  ChannelReset), `unsupported_template` — если пока только сохранили hex-дамп.

Семантика ключевых событий:
- **EmptyBook (templateId=9)** — сигнал «книга инструмента очищена». После него по
  этому `security_id` приходит новая нитка `Order_MBO` с флагом recovery, и `rptSeq`
  для инструмента начинается с 1. При получении EmptyBook нужно очистить локальное
  состояние и ждать новых заявок.
- **ChannelReset (templateId=11)** — сброс всего канала. Требуется очистить книги
  всех инструментов конкретного канала/ноги. После ChannelReset биржа рассылает
  EmptyBook + recovery-поток для каждого инструмента; snapshot feed тоже
  переинициализируется.

### `errors.parquet` — ошибки парсинга

Описание: любое отклонение от протокола/сбоев чтения.

Колонки:
- `source_file` — PCAP, где произошла ошибка.
- `stage` — этап (pcap/packet_header/framing/message).
- `packet_index_in_file` — порядковый номер пакета в файле (0-based).
- `message_index_in_packet` — номер сообщения внутри пакета.
- `template_id` — TemplateId Problematic message (если был прочитан).
- `error_code` — краткий код (`udp_parse`, `message_bounds`, `schema_mismatch`, ...).
- `error_text` — подробное описание.
- `raw_context_hex` — hex-контекст (фрагмент payload).

Дополнительно Python-слой может строить derived таблицы (updates/book), но они
живут поверх базовых parquet'ов.


Проверка incremental_orders

  - data/parsed_flatten/incremental_orders/channel_hint=78/security_id=200001478879/feed_leg=A|B/part-0.parquet: на A 894 451 строк, на B 733 589 (разница 160 862). После исключения сугубо транспортных колонок
  (source_file, feed_leg, channel_hint, packet_sequence_number, packet_sending_time_ns, message_index_in_packet) 160 862 записей остаются только в фиде A; на фиде B уникальных записей не нашлось. Пример события,
  встречающегося лишь в A: {'security_id': 200001478879, 'md_update_action': 'add', 'side': 'buy', 'price': 5774.0, 'size': 1, 'position_no': 46, 'entering_firm': 120, 'md_insert_timestamp_ns': 1731938447698452951,
  'secondary_order_id': 783709289669, 'rpt_seq': 1831261, 'md_entry_timestamp_ns': 1731938447698497469}.
  - data/parsed_flatten/incremental_orders/channel_hint=88/security_id=200001679383/feed_leg=A|B/part-0.parquet: на A 44 247 строк, на B 48 895, то есть на B на 4 648 событий больше. После той же нормализации все 4 648
  записей остались только в B (уникальных строк у A нет). Пример B-only события: {'security_id': 200001679383, 'md_update_action': 'add', 'side': 'buy', 'price': 124.12, 'size': 1, 'position_no': 36, 'entering_firm': 8,
  'md_insert_timestamp_ns': 1731936042976589572, 'secondary_order_id': 8842720633415, 'rpt_seq': 73946, 'md_entry_timestamp_ns': 1731936042976634068}.

  Как сравнивал

  - Для каждого фида считывал part-0.parquet через PyArrow (pq.ParquetFile), удалял перечисленные выше колонки, превращал оставшиеся бизнес-поля в мультимножество (Counter) и искал разницу. DuckDB в активном окружении /
  mnt/conda/envs/simulator по-прежнему не импортируется, поэтому вычисления делал в Arrow/Pandas.

  Что дальше

  1. Повторить сравнение для других инструментов и, при необходимости, других таблиц (snapshot_*, incremental_*) — это покажет масштаб расхождений между каналами.
  2. Пройти по цепочке загрузки → нормализации, чтобы понять, на каком этапе пропадают целые сообщения на одном из фидов; после исправления переинициализировать data/parsed_flatten.



