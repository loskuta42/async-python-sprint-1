# Анализатор данных погодных условий, которые берутся из API Яндекс Погоды

Основные этапы выполняемые разработанным приложением

**1. Получение информацию о погодных условиях для указанного списка городов, используя API Яндекс Погоды (класс `tasks.DataFetchingTask`).**

<details>
<summary> Описание </summary>

За выполнение данной подзадачи отвечает класс(отнаследованный от класса Process)-producer `DataFetchingTask` внутри которого выполняются запросы в пуле потоков, и добавляются в очередь на вычисление основных показателей погоды классом-consumer-ом `DataCalculationTask`.

Список городов находится в переменной `CITIES` в файле [utils.py](utils.py). Для взаимодействия с API использовался предоставленный создателями курса готовый класс `YandexWeatherAPI` в модуле `api_client.py`. Пример работы с классом `YandexWeatherAPI` описан в <a href="#apiusingexample">примере</a>. Пример ответа от API для анализа находится в [файле](examples/response.json).

</details>

**2. Вычисление средней температуры и предварительный анализ информации об осадках за указанный период для всех городов (класс `tasks.DataCalculationTask`).**

<details>
<summary> Описание </summary>

Особенности предварительного анализа:
- период вычислений в течение дня — с 9 до 19 часов;
- средняя температура рассчитывается за указанный промежуток времени;
- сумма времени (часов), когда погода без осадков (без дождя, снега, града или грозы), рассчитывается за указанный промежуток времени;
- информация о температуре для указанного дня за определённый час находится по следующему пути: `forecasts> [день]> hours> temp`;
- информация об осадках для указанного дня за определённый час находится по следующему пути: `forecasts> [день]> hours> condition`.

[Пример данных](examples/response-day-info.png) с информацией о температуре и осадках за день.

Список вариантов погодных условий находится [в таблице в блоке `condition`](https://yandex.ru/dev/weather/doc/dg/concepts/forecast-test.html#resp-format__forecasts) или в [файле](examples/conditions.txt).

</details>

**3. Объединение полученных данных и сохрание результата в текстовом файле (класс `tasks.DataAggregationTask`).**

<details>
<summary> Описание </summary>

Формат сохраняемого файла - **json**.

Формат таблицы для сохранения. 

| Город/день |                           | 14-06 | ... | 19-06 | Среднее | Рейтинг |
|-------------|:--------------------------|:-----:|:---:|:-----:|--------:|--------:|
| Москва      | Температура, среднее      |  24   |     |  27   |    25.6 |       8 |
|             | Без осадков, часов        |  10   |     |   7   |     9.5 |         |
| Абу-Даби    | Температура, среднее      |  34   |     |  37   |    35.5 |       2 |
|             | Без осадков, часов        |  18   |     |  15   |    16.5 |         | 
| ...         |                           |       |     |       |         |         |

</details>


**4. Анализ результата и вывод о том, какой из городов наиболее благоприятен для поездки (класс `tasks.DataAnalyzingTask`).**

<details>
<summary> Описание </summary>

Наиболее благоприятным городом считать тот, в котором средняя температура за всё время была самой высокой, а количество времени без осадков — максимальным.
Если таких городов более одного, то выводятся все.

</details>

## Особенности приложения:

1. Используются как процессы, так и потоки.
2. Используются как очередь, так и пул задач.
3. Используются концепции ООП.

---

<a name="apiusingexample"></a>

## Пример использования `YandexWeatherAPI` для работы с API

```python
from api_client import YandexWeatherAPI

city_name = "MOSCOW"
ywAPI = YandexWeatherAPI()
resp = ywAPI.get_forecasting(city_name)
```
