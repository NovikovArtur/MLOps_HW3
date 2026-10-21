# HW3

Для запуска нужны Docker Compose и файлы `fraud_detector/teta-ml-1-2025/train.csv`, `fraud_detector/teta-ml-1-2025/test.csv` из [набора транзакций](https://www.kaggle.com/competitions/teta-ml-1-2025). CSV не входят в репозиторий.

```bash
docker compose up --build -d
docker compose ps
```

Интерфейс скоринга: [localhost:8501](http://localhost:8501). Airflow: [localhost:8080](http://localhost:8080), логин `admin`, пароль `admin`.

В Airflow доступны `append_labeled_month`, `retrain_fraud_model`, `check_data_quality`. Для проверки запустите их вручную в этом порядке через кнопку Trigger DAG. Переобучение также идёт каждые 5 минут, пополнение витрины каждый месяц, Data Quality ежедневно. У всех DAG выключен `catchup`.

После успешного переобучения проверьте `fraud_detector/models/active.json` и новый файл в `fraud_detector/models/`. Загрузите `fraud_detector/teta-ml-1-2025/test.csv` через интерфейс скоринга. Версию модели видно в результатах PostgreSQL и сообщениях Kafka:

```bash
docker compose exec -T postgres psql -U fraud -d fraud -c "SELECT transaction_id, score, fraud_flag, model_version FROM transaction_scores ORDER BY scored_at DESC LIMIT 10;"
```

Отчёт Data Quality: `fraud_detector/reports/latest.html`.

Витрина хранит 36 месяцев. За один запуск добавляется 100000 строк. Окно обучения содержит 3 месяца: два ранних для обучения, последний для проверки. Повторный запуск не добавляет тот же месяц снова. Указатель месяца обновляется после публикации модели. Скоринг подхватывает новую версию без перезапуска.

Метрика сравнения: average precision на последнем месяце окна. Новая и текущая модели оцениваются на одинаковых строках. Исходная модель служит только для запуска скоринга и не участвует в первом сравнении. При снижении метрики публикация отменяется, версия и указатель месяца не меняются.

Правила Data Quality: обязательные поля заполнены; метка равна 0 или 1; сумма неотрицательна и конечна; координаты допустимы; дата соответствует месяцу витрины; категория известна препроцессингу.
