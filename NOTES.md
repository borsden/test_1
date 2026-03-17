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

> **PCAP** — это бинарный дамп сетевого трафика (формат libpcap). Каждая запись —
> Ethernet кадр → IP заголовок → UDP заголовок → полезная нагрузка UMDF. Сниффер
> может захватывать и прочие пакеты (ARP, IGMP, служебные IP), но декодер
> отбрасывает всё, что не совпадает по адресу/порту с нужным UMDF-каналом. Мы
> читаем эти UDP-пакеты так, словно получаем их по multicast вживую, поэтому
> события нужно применять строго по порядку без пропусков: PCAP уже содержит
> последовательный захват биржевого потока.

- `*_Instrument.pcap` — справочник инструментов.
- `*_Snapshot.pcap` — снапшоты книги.
- `*_Incremental_feedA.pcap`, `*_Incremental_feedB.pcap` — две ноги
  инкрементального потока. Это параллельные multicast'ы c идентичным содержимым,
  отличие только в сетевом адресе (A/B).

Каналы 78 и 88 — независимые multicast-потоки с разными группами инструментов.
FeedA и FeedB внутри канала должны быть эквивалентны (это дубли для
отказоустойчивости).

### Каналы и фиды

- **Канал** (`channel_78`, `channel_88`, …) — отдельный multicast-поток UMDF,
  внутри которого торгуется фиксированный набор инструментов. Все выпускаемые
  таблицы верхнего уровня (`instruments`, `snapshot_*`) группируются по каналу.
- **Фид** (`feed_A`, `feed_B`) — конкретная нога инкрементального потока внутри
  канала. Биржа рассылает одинаковые market-data события по двум multicast-адресам,
  чтобы потребитель мог переключаться при потере пакетов.
- **Идентичность данных.** Мы эксплицитно проверяем, что содержимое `feed_A` и
  `feed_B` совпадает для всех инкрементальных таблиц в рамках одного канала
  (тест `tests/test_decoder.py::test_feed_legs_match`). Этот тест гоняется в CI и
  гарантирует, что декодер не смешивает события между ногами и что обе копии
  полностью пересекаются.

## Структура выходных данных

`b3sbe_decode` пишет результат в иерархию каталогов:

```
parsed/
  channel_78/
    instruments.parquet
    instrument_legs.parquet
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

Каталог `channel_XX` отвечает конкретному БЗ-каналу UMDF, внутри него лежат
агрегированные таблицы (`instruments`, `snapshot_*`). Подкаталог `feed_A|B` —
копия инкрементального потока с соответствующей ноги (feed).

Ниже перечислены доступные таблицы и их поля.

## Какие Parquet-таблицы пишем

Для каждого файла указываем источник и назначение, а также набор колонок.

### `instruments.parquet` — `SecurityDefinition_12`

Описание: справочник инструментов (одна строка на `security_id`).

Колонки:
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
- `security_id` — родительский инструмент.
- `packet_sequence_number` — последовательность пакета.
- `underlying_security_id` — ID базового инструмента.
- `underlying_security_exchange` — биржа базового.
- `underlying_symbol` — тикер базового.
- `position_in_group` — порядок в repeating group (0..N-1).

### `instrument_legs.parquet` — группа `noLegs`

Описание: составные ноги стратегий/спредов.

Колонки:
- `security_id`, `packet_sequence_number` — lineage.
- `leg_security_id` — ID ноги.
- `leg_symbol` — тикер ноги.
- `leg_side_raw`, `leg_side` — сторона (BUY=1, SELL=2) и строковая форма.
- `leg_ratio_qty_mantissa`, `leg_ratio_qty` — коэффициент количества (int64, double).
- `position_in_group` — позиция в `noLegs`.

### `instrument_attributes.parquet` — группа `noInstrAttribs`

Описание: дополнительные атрибуты инструмента (канальные флаги, особенности
торговли и т.д.).

Колонки:
- `security_id`, `packet_sequence_number`.
- `attribute_type` — числовой код `InstrAttribType`.
- `attribute_value` — строковое значение (через enum или fallback `str(value)`).
- `position_in_group` — индекс в `noInstrAttribs`.

### `instrument_other.parquet` — прочие сообщения instrument-потока

Описание: служебные сообщения в Instrument Definition stream, которые пока не
имеют отдельной таблицы (например, `SequenceReset`, возможные уведомления о
состоянии группы инструментов).

Колонки совпадают с `incremental_other`: `template_id`, `template_name`,
`security_id`, `rpt_seq`, `match_event_indicator_raw`, `md_entry_timestamp_ns`,
`packet_sequence_number`, `packet_sending_time_ns`, `body_hex`, `decode_status`.

Типичные шаблоны:
- **SequenceReset (templateId=1)** — завершает цикл публикаций в исходном
  фиде, задаёт новый `SequenceNumber`.
- Прочие уведомления, которые биржа может вставлять в instrument stream
  (например, редкие статусы по группам инструментов).

### `snapshot_headers.parquet` — `SnapshotFullRefresh_Header_30`

Описание: метаданные снапшота книги для `security_id` и канала.

Колонки:
- `packet_sequence_number`, `packet_sending_time_ns` — PacketHeader.
- `security_id` — инструмент снапшота.
- `last_msg_seq_num_processed` — последний rptSeq применённый на бирже.
- `tot_num_bids`, `tot_num_offers` — количество заявок в снапшоте.
- `last_rpt_seq` — rptSeq последнего snapshot entry.
- `snapshot_header_row_id` — уникальный surrogate-ключ.

### `snapshot_orders.parquet` — `SnapshotFullRefresh_Orders_MBO_71`

Описание: сами заявки из снапшота (multilevel order book).

Колонки:
- `packet_sequence_number`, `packet_sending_time_ns` — PacketHeader.
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

### `snapshot_other.parquet` — прочие snapshot-шаблоны

Описание: все `templateId`, которые приходят в snapshot-потоке и пока не имеют
собственной таблицы (SecurityStatus, SecurityGroupPhase, PriceBand,
QuantityBand, High/Low/LastTradePrice, ExecutionStatistics, SettlementPrice и т.д.).
Поскольку снапшот не делится на feed A/B, файлы лежат непосредственно в корне
`channel_XX`.

Колонки совпадают с `incremental_other` (см. ниже):
- `template_id`, `template_name`, `security_id`, `rpt_seq` (если присутствует в
  сообщении).
- `match_event_indicator_raw`, `md_entry_timestamp_ns`.
- `packet_sequence_number`, `packet_sending_time_ns`.
- `body_hex`, `decode_status` (пока `unsupported_template`).

### `incremental_orders.parquet` — `Order_MBO_50`

Описание: добавление/изменение заявки в инкрементальном потоке.

Колонки:
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
- `packet_sequence_number`, `packet_sending_time_ns`.
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

Описание: массовые удаления позиций (wipe стеков).

Колонки:
- `packet_sequence_number`, `packet_sending_time_ns`.
- `security_id` — инструмент.
- `match_event_indicator_raw` — флаги события.
- `md_update_action_raw`, `md_update_action` — действие (обычно `delete_from`).
- `md_entry_type_raw`, `side` — тип записи/сторона.
- `position_no` — номер позиции (`mDEntryPositionNo` в UMDF).
- `rpt_seq` — счётчик событий.
- `md_entry_timestamp_ns` — временная метка сообщения.

> **Schema v9 (важно):** Биржа **не** передаёт отдельные `start/end` поля. Направление удаления определяется `md_update_action`:
> - `DELETE_THRU` — мгновенно очищает всю сторону (в UMDF v9 `position_no` всегда = 1).
> - `DELETE_FROM` — удаляет верхнюю часть стакана **с 1 по `position_no` включительно**. Чтобы не слать сотни `DeleteOrder`, движок присылает один `DELETE_FROM`, когда агрессор «съел» N первых уровней.
> - Другие действия в v9 не встречаются; если появятся — логируем warning и игнорируем.

### `incremental_trades.parquet` — `Trade_53`

Описание: события сделок (Volume prints).

Колонки:
- `packet_sequence_number`, `packet_sending_time_ns`.
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
- `packet_sequence_number`, `packet_sending_time_ns`.
- `security_id` — какая книга очищена.
- `match_event_indicator_raw` — биты события (5-й бит = recovery message).
- `md_entry_timestamp_ns` — временная метка EmptyBook.

### `incremental_channel_resets.parquet` — `ChannelReset_11`

Описание: сброс всего канала/ноги. После ChannelReset клиент обязан очистить
все книги по этому каналу, дождаться `EmptyBook` для нужных инструментов и
применить новый recovery-поток. Snapshot feed для канала тоже переинициализируется.

Колонки:
- `packet_sequence_number`, `packet_sending_time_ns`.
- `match_event_indicator_raw` — биты события (показывают recovery).
- `md_entry_timestamp_ns` — метка времени.

### `incremental_other.parquet` — прочие инкрементальные шаблоны

Описание: все templateId из инкрементального потока, для которых ещё нет
typed-таблицы (SecurityStatus, PriceBand, ExecutionStatistics и т.д.). Snapshot
и instrument аналоги этих сообщений живут в `snapshot_other.parquet` и
`instrument_other.parquet` соответственно.

- Колонки:
- `template_id`, `template_name` — идентификатор и человекочитаемое имя темплейта.
- `security_id` — инструмент (для чисто каналовых событий значение может быть 0).
- `rpt_seq` — rptSeq, если сообщение его вообще публикует (у части шаблонов поле отсутствует).
- `match_event_indicator_raw` — биты matchEventIndicator (важно для recovery flag).
- `md_entry_timestamp_ns` — временная метка события.
- `packet_sequence_number`, `packet_sending_time_ns` — PacketHeader.
- `body_hex` — hex-дамп тела SBE.
- `decode_status` — `unsupported_template`, пока мы сохраняем только сырой hex.

Семантика ключевых событий:
- **SecurityStatus (templateId=3)** — включает текущее состояние конкретного
  инструмента (PAUSE, OPEN и т.д.) и причину изменения (`SecurityTradingEvent`).
- **SecurityGroupPhase (templateId=10)** — переводит целую группу инструментов в
  новую фазу (`tradingSessionSubID`), клиенту нужно применить статус ко всем
  securityId из группы.
- **OpeningPrice/ClosingPrice (templateId=15,17)** — итоговые цены/объёмы сессии
  открытия/закрытия и связанные статистики.
- **PriceBand/QuantityBand (templateId=22,21)** — параметры тоннелей и лимитов по
  цене/объёму (Hard/Reject/Auction/Static), содержат границы и служебные флаги.
- **Misc. statistics**: LastTradePrice (27), HighPrice/LowPrice (24/25),
  SettlementPrice (28), OpenInterest (29), ExecutionStatistics (56) — публикуют
  отдельные рыночные показатели (последняя цена, hi/lo за сессию, теоретическая и
  расчётная цена, открытый интерес, VWAP/объём).

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

## Шаблоны с rptSeq, требующие реализации

Для строгого контроля последовательностей rptSeq нужно обрабатывать все шаблоны,
в которых присутствует это поле. Сейчас реализованы только
orderbook-критичные сообщения (Order/Delete/MassDelete/Trade/ForwardTrade/
ExecutionSummary/ExecutionStatistics/TradeBust), а остальные 13 шаблонов попадают
в `NonBookMessage`. Из-за этого при реплее они считаются «прочими» и не влияют
на логику контроля разрывов.

Нереализованные шаблоны с rptSeq:

1. SecurityStatus_3 (id 3)
2. OpeningPrice_15 (15)
3. TheoreticalOpeningPrice_16 (16)
4. ClosingPrice_17 (17)
5. AuctionImbalance_19 (19)
6. PriceBand_20 (20)
7. QuantityBand_21 (21)
8. PriceBand_22 (22)
9. HighPrice_24 (24)
10. LowPrice_25 (25)
11. LastTradePrice_27 (27)
12. SettlementPrice_28 (28)
13. OpenInterest_29 (29)
