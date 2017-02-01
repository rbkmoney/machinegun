# MG

Machinegun (или сокращённо MG) — сервис для упрощение реализации бизнес процессов, которые удобно моделировать через автоматную логику (см. [Теорию автоматов](https://en.wikipedia.org/wiki/Automata_theory)).

# Текущее состояние

В данный момент в эксплуатацию запускать ещё нельзя.

До запуска нужно ещё сделать следующие вещи:

1. сделать прокидывание woody контекста по всей цепочке обработки (в том числе при обращении в базу, и логировать все события с контекстом)
1. EventNotFound (?)

Кроме того нужно сделать:

1. удаление машины, старых эвентов, тэгов
1. сделать deadline у запросов к машине (до какого момента запрос актуален)
1. кэширование стейта в памяти
1. выгрузка машин при нехватке памяти
1. сделать хранение эвентов в кэше ets'е в с lru (?)
1. сделать стресс-тест


Так же нужен рефакторинг:

1. привести в порядок работу с NS
1. перенести в genlib utils
1. вынести automaton часть из mg_machine
1. привести в порядок настройки тестов
1. переливка в эвент синк и тэгирование на уровень выше events_machine


# Вопросы

1. Можно ли получить историю у упавшей машины? — Да, можно, т.к. падение машины влияет только на прогресс.


# Описание

!!! attention "Todo"

## Машина и автоматон

!!! attention "Todo"

## Работа с хранилищем

!!! attention "Todo"

## Воркеры

!!! attention "Todo"

## Распределённость

!!! attention "Todo"

## Обработка нестандартных ситуаций

Одна из основных задач MG — это взять на себя сложности по обработке нестандартных ситуаций (_НС_), чтобы бизнес-логика мога концентрироваться на своих задачах.
_НС_ — это любое отклонение от основного бизнес процесса (в запросе неправильные данные, отключилось питание, в коде ошибка).
Очень важный момент, что все _НС_ как можно быстрее должны детектироваться, записываться в журнал и обрабатываться.


### Неправильные запросы

Самый простой случай _НС_ — это когда нам посылают некорректный запрос. В таком случае в интерфейсе предусмотрены специальные коды ответа.
Например:
 * машина не найдена
 * машина уже создана
 * машина _сломалась_


### Недоступность других сервисов и таймауты

!!! attention "Todo обновить"

Сервисы от которых зависим (хранилище, процессор), могут быть не доступны. Это штатная ситуация, и есть понимание, что это временно (например, пришло слишком много запросов в один момент времени), и система должна прийти в корректное состояние через некоторое время (возможно с помощью админов).

В таком случае возникает два варианта развития событий:

 1. если есть возможность ответить недоступность клиенту, то нужно это сделать (есть контекст запроса start, call, get, repair)
 1. если ответить недоступностью нельзя (некому, timeout), то нужно бесконечно с [экспоненциальной задержкой](https://en.wikipedia.org/wiki/Exponential_backoff) делать попытки. Такие запросы должны быть идемпотентными (почему, см. ниже).

Отдельным вариантом недоступности является таймаут, когда запрос не укладывается в заданный промежуток времени. Главное отличие этой ситуации от явной недоступности в том, что нет информации о том, выполнился ли запрос на другой стороне или нет. Как следствие повторить запрос можно только в том случае если есть гарантия, что запрос идемпотентный.


### Недостаточность ресурсов и внутренние таймауты

Возможна ситуация когда запросов больше чем есть ресурсов для их обработки.

Проявлятся это может 2мя способами (а может больше?):

1. нехватка памяти
1. рост очередей у процессов обработчиков

Для мониторинга памяти нужно при каждом запросе смотреть, что есть свободная память.

Для мониторинга очередей нужно при каждом запросе к обработчику (сокет принятия запроса, воркер машины, процесс доступа к хранилищу или процессору) смотреть на размер его очереди и проверять, не слишком ли она длинная.

В каждом случае при детектирования нехватки ресурсов нужно отвечать ошибкой временной недоступности.


### Неожиданное поведение

Никто не идеален и возможна ситуация, что какой-то компонент системы ведёт себя не так, как ожидалось. Это очень важно обнаружить как можно раньше, чтобы минимизировать возможный ущерб.

Если неожиданное поведение детектируется в контексте запроса, то нужно отдать этот факт в запрос.

Если контекста запроса нет, то нужно переводить соответствующую машину в состояние _неисправности_ и ожидать.

Любой такой случай — это предмет для вмешательства оператора, разбирательства и внесение корректировок.


### Отключения ноды

Ситуация, что нода на которой работает экземпляр сервиса перестанет работать очень вероятна (чем больше кластер, тем больше вероятность). Поэтому нужно понимать поведение в такой момент. 

Возникнуть такое может в любой момент — это важно, т.к. нужно писать весь код думая об этом. И описать одну схему обработки таких ситуаций сложно, поэтому выделим основную, и если она не подходит, то нужно думать отдельно.

Все действия машины должны быть идемпотентными. И если поток выполнения прервётся в процессе обработки, то можно безопасно повторить ещё раз выполненную часть.

### Сплит кластера

!!! attention "Todo"


## EventSink

Основная его задача — сохранение сплошного потока эвенотов, для возможности синхронизации баз. Эвенты должны быть total ordered, и должна быть цепочка хэшей для контроля целостности.
Находится отдельно от машин, и может быть подписан на произвольные namespace'ы. Тоже является машиной в отдельном нэймспейсе (чтобы это работало нормально нужно сделать [оптимизации протокола](https://github.com/rbkmoney/damsel/pull/38) и возможно отдельный бэкенд для бд).
Через настройки описываются подписки event_sink'ов на namespace'ы (точнее на машины).
У машины появляется промежуточный стейт для слива в синк.
