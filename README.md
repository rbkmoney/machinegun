# mg

A service that does something

## Сборка

Для запуска процесса сборки достаточно выполнить просто:

    make

Чтобы запустить полученную сборку в режиме разработки и получить стандартный [Erlang shell][1], нужно всего лишь:

    make start

> _Хозяйке на заметку._ При этом используется стандартный Erlang релиз, собранный при помощи [relx][2] в режиме разработчика.

## Документация

Дальнейшую документацию можно почерпнуть, пройдясь по ссылкам в [соответствующем документе](doc/index.md). 

[1]: http://erlang.org/doc/man/shell.html
[2]: https://github.com/erlware/relx

## Хочется ещё сделать

в текущей версии:
1. убрать GetLastEventID и добавить направление в history_range

в следующей версии:
1. детектировать двойное тегирование
1. автоподнятие машин
1. сделать сохранение эвента в синк отдельным шагом автомата
1. сделать контроль целостности в event_sink'е
1. подключить реальную БД

рефакторинг который нужно делать:
1. привести в порядок работу с exceptions
1. разбить тесты на разные машины
1. привести в порядок настройки тестов
1. доработать readme

тесты:
1. сделать стресс-тест


## Вопросы

1. Можно ли получить историю у упавшей машины? Да, можно, т.к. падение машины влияет только на прогресс.


## EventSink

Основная его задача — сохранение сплошного потока эвенотов, для возможности синхронизации баз. Эвенты должны быть total ordered, и должна быть цепочка хэшей для контроля целостности.
Находится отдельно от машин, и может быть подписан на произвольные namespace'ы. Тоже является машиной в отдельном нэймспейсе (чтобы это работало нормально нужно сделать [оптимизации протокола](https://github.com/rbkmoney/damsel/pull/38) и возможно отдельный бэкенд для бд).
Через настройки описываются подписки event_sink'ов на namespace'ы (точнее на машины).
У машины появляется промежуточный стейт для слива в синк.
