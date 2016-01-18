# pathman

The `pathman` module provides optimized partitioning mechanism and functions to manage partitions.

## pathman Concepts

Partitioning refers to splitting one large table into smaller pieces. Each row in such table assigns to a single partition based on partitioning key. Common partitioning strategies are:

* HASH - maps rows to partitions based on hash function values;
* RANGE - maps data to partitions based on ranges that you establish for each partition;
* LIST - maps data to partitions based on explicitly specified values of partitioning key for each partition.

PostgreSQL supports partitioning via table inheritance. Each partition must be created as child table with CHECK CONSTRAINT. For example:

```
CHECK ( id >= 100 AND id < 200 )
CHECK ( id >= 200 AND id < 300 )
```

Despite the flexibility of this approach it has weakness. If query uses filtering the optimizer forced to perform an exhaustive search and check constraints for each partition to determine partitions from which it should select data. If the number of partitions is large the overhead may be significant.

The `pathman` module provides functions to manage partitions and partitioning mechanism optimized based on knowledge of the partitions structure. It stores partitioning configuration in the `pathman_config` table, each row of which contains single entry for partitioned table (relation name, partitioning key and type). During initialization the `pathman` module caches information about child partitions in shared memory in form convenient to perform rapid search. When user executes SELECT query pathman analyzes conditions tree looking for conditions like:

```
VARIABLE OP CONST
```
where `VARIABLE` is partitioning key, `OP` is comparison operator (supported operators are =, <, <=, >, >=), `CONST` is scalar value. For example:

```
WHERE id = 150
```

Based on partitioning type and operator the `pathman` searches corresponding partitions and builds the plan.

## Pathman Functions

### Partitions Creation
```
CREATE FUNCTION create_hash_partitions(
    relation TEXT,
    attribute TEXT,
    partitions_count INTEGER)
```
Performs HASH partitioning for `relation` by integer key `attribute`. Creates `partitions_count` partitions and trigger on INSERT. Data doesn't automatically copied from parent table to partitions. Use `partition_data()` function (see below) to migrate data.

```
CREATE FUNCTION create_range_partitions(
    relation TEXT,
    attribute TEXT,
    start_value ANYELEMENT,
    interval ANYELEMENT,
    premake INTEGER)
```
Performs RANGE partitioning for `relation` by partitioning key `attribute`. `start_value` argument specifies initial value, `interval` sets the range of values in a single partition, `premake` is the number of premade partitions (the only one partition will be created if `premake` is 0).
```
CREATE FUNCTION create_range_partitions(
    relation TEXT,
    attribute TEXT,
    start_value ANYELEMENT,
    interval INTERVAL,
    premake INTEGER)
```
Same as above but suitable for `DATE` and `TIMESTAMP` partitioning keys.

### Data migration
```
CREATE FUNCTION partition_data(parent text)
```
Copies data from parent table to its partitions.

### Partitions management
```
CREATE FUNCTION split_range_partition(partition TEXT, value ANYELEMENT)
```
Splits RANGE `partition` in two by `value`.
```
CREATE FUNCTION merge_range_partitions(partition1 TEXT, partition2 TEXT)
```
Merge two adjacent RANGE partitions. Данные из `partition2` копируются в `partition1`, после чего секция `partition2` удаляется.
```
CREATE FUNCTION append_partition(p_relation TEXT)
```
Добавляет новую секцию в конец списка секций. Диапазон значений устанавливается равным последней секции.
```
CREATE FUNCTION prepend_partition(p_relation TEXT)
```
Добавляет новую секцию в начало списка секций.
```
CREATE FUNCTION disable_partitioning(relation TEXT)
```
Отключает механизм секционирования `pathman` для заданной таблицы и удаляет триггер на вставку. При этом созданные ранее секции остаются без изменений.

## Примеры использования
### HASH
Рассмотрим пример секционирования таблицы, используя HASH-стратегию на примере таблицы.
```
CREATE TABLE hash_rel (
    id      SERIAL PRIMARY KEY,
    value   INTEGER);
INSERT INTO hash_rel (value) SELECT g FROM generate_series(1, 10000) as g;
```
Разобьем таблицу `hash_rel` на 100 секций по полю `value`:
```
SELECT create_hash_partitions('hash_rel', 'value', 100);
```
Перенсем данные из родительской таблицы в дочерние секции.
```
SELECT partition_data('hash_rel');
```
### RANGE
Пример секционирования таблицы с использованием стратегии RANGE.
```
CREATE TABLE range_rel (
    id SERIAL PRIMARY KEY,
    dt TIMESTAMP);
INSERT INTO range_rel (dt) SELECT g FROM generate_series('2010-01-01'::date, '2015-12-31'::date, '1 day') as g;
```
Разобьем таблицу на 60 секций так, чтобы каждая секция содержала данные за один месяц:
```
SELECT create_range_partitions('range_rel', 'dt', '2010-01-01'::date, '1 month'::interval, 59);
```
> Значение `premake` равно 59, а не 60, т.к. 1 секция создается независимо от значения `premake`

Перенсем данные из родительской таблицы в дочерние секции.
```
SELECT partition_data('range_rel');
```
Объединим секции первые две секции:
```
SELECT merge_range_partitions('range_rel_1', 'range_rel_2');
```
Разделим первую секцию на две по дате '2010-02-15':
```
SELECT split_range_partition('range_rel_1', '2010-02-15'::date);
```
Добавим новую секцию в конец списка секций:
```
SELECT append_partition('range_rel')
```