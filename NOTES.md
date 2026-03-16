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

### `incremental_other.parquet` — прочие инкрементальные шаблоны

Описание: все templateId, для которых ещё нет typed-таблицы (SecurityStatus,
PriceBand, Sequence, News и т.д.).

Колонки:
- `source_file` — исходный PCAP.
- `template_id` — TemplateId сообщения.
- `template_name` — известное имя (если перечислено в `template_name()`), иначе `unknown`.
- `security_id` — значение поля `securityID`, если оно присутствует (иначе 0).
- `rpt_seq` — rptSeq из сообщения (если присутствует).
- `packet_sequence_number`, `packet_sending_time_ns` — PacketHeader.
- `body_hex` — hex-дамп тела сообщения.
- `decode_status` — `unsupported_template` или текст ошибки.

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
