# Apache Airflow практикалық тапсырма

`Airflow_Tapsyrma_KZ.docx` құжатындағы барлық 17 тапсырманың шешімі.
Код файлдарында комментарий және docstring жоқ. Міндетті `doc_md`
Airflow интерфейсінде оқылатын DAG сипаттамасын сақтайды.

Тексерілді: **27 тест өтті**, екі нақты DAG іске қосылуы да **SUCCESS**.
Есеп: `output/final_report.txt`. Тексеру деректері: `results/validation.md`.

## Жоба құрылымы

```text
airflow-homework/
├── .github/workflows/check_airflow.yml
├── dags/popular_people_pipeline.py
├── spark_jobs/transformations.py
├── data/popular_people.csv
├── scripts/run.sh
├── scripts/run_dags.py
├── tests/test_dag_structure.py
├── tests/test_tasks.py
├── requirements.txt
├── run_wsl.ps1
└── README.md
```

## Windows-та іске қосу

Airflow үшін WSL Ubuntu пайдаланылады. Осы компьютерде дайындалған
тексеру ортасы WSL ішіндегі `~/.cache/codex-airflow-homework-runtime`
қалтасында сақталады. Жоба қалтасынан PowerShell командалары:

```powershell
.\run_wsl.ps1 test
.\run_wsl.ps1 verify
```

`test` автотесттерді орындайды. `verify` екі DAG-ті де нақты Airflow
тапсырмалары мен XCom арқылы іске қосып, орындалу күйін тексереді.

Веб-интерфейсті ашу үшін алдымен пайдаланушы жасаңыз:

```powershell
.\run_wsl.ps1 init
```

Содан кейін екі бөлек терминалда:

```powershell
.\run_wsl.ps1 webserver
```

```powershell
.\run_wsl.ps1 scheduler
```

Мекенжай: http://localhost:8080. Оқу ортасындағы кіру деректері: admin / admin.
Интерфейсте екі DAG-ті көресіз: `popular_people_pipeline` және
`popular_people_taskflow`. Кестемен орындау үшін қажетті DAG-ті қосыңыз.
`SequentialExecutor` тапсырмаларды кезекпен орындайды; графтағы параллель
тармақтардың тәуелділіктері сақталады.

## Басқа Linux немесе WSL ортасына орнату

Python 3.10 және Java 11 қажет. Жоба қалтасында:

```bash
python3.10 -m venv .venv
source .venv/bin/activate
python -m pip install apache-airflow==2.8.0 --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.8.0/constraints-3.10.txt
python -m pip install -r requirements.txt
bash scripts/run.sh test
bash scripts/run.sh verify
```

`JAVA_HOME` Java 11 орналасқан қалтаға бағытталсын. Airflow алдымен ресми
constraints файлымен орнатылады; кейін тапсырмада берілген Spark provider,
PySpark және тест кітапханаларының нақты нұсқалары қосылады.
Python 3.14 немесе Airflow 3 нұсқасын осы жоба үшін пайдаланбаңыз.

## 17 тапсырма

| № | Орындалған жұмыс |
|---|---|
| 1 | popular_people_pipeline, @daily, catchup=False, owner, retries=2, 3 минут retry_delay, tags |
| 2 | EmptyOperator арқылы start және end |
| 3 | CSV бар-жоғын және 9000-нан көп жолын тексеру, XCom нәтиже |
| 4 | CSV тақырыбындағы бес міндетті бағанды тексеру |
| 5 | AirflowPopularPeople, local[*], жол саны, бағандар және Spark нұсқасы |
| 6 | Тазалау, gender_label, popularity_tier, cleaned Parquet және XCom жол саны |
| 7 | Салалар, жыныстар және ROW_NUMBER арқылы саладағы TOP-3 үшін үш Parquet есеп |
| 8 | repartition(4) және сала бойынша partitionBy |
| 9 | Жалпы жол саны, ТОП-5 сала, орташа танымалдылық, жасалған уақыт бар мәтіндік есеп |
| 10 | BranchPythonOperator: >100 success, қалғаны warning |
| 11 | Нақты N мәнімен екі хабарлама және end-ке қосылу |
| 12 | Құжаттағы толық тәуелділік графы |
| 13 | generate_report ішінде spark_clean return_value XCom пайдалану |
| 14 | @dag және @task арқылы екінші TaskFlow DAG |
| 15 | Жалпы SLA 1 сағат, spark_clean SLA 30 минут |
| 16 | spark_load үшін retries=3, 30 секунд, exponential backoff |
| 17 | Кіріс, шығыс, әрекеттер және кесте сипатталған doc_md |

## SLA және retry түсіндірмесі

SLA — тапсырманың күтілген аяқталу мерзімі. Мерзім асып кетсе,
Airflow SLA miss ретінде тіркей алады; SLA өзі тапсырманы тоқтататын
таймаут емес. Жоспарланған іске қосылулар үшін жалпы 1 сағат,
spark_clean үшін 30 минут орнатылған. Қолмен іске қосылған DAG тексерісі
жоспарланған SLA мониторингінің дәлелі болып саналмайды.

Spark тапсырмалары уақытша файлдық жүйе немесе орындау ортасы қатесінен
сәтсіз аяқталуы мүмкін. Retry тапсырманы қайта орындауға мүмкіндік береді.
Exponential backoff кезекті әрекеттердің аралығын ұлғайтады.
Parquet нәтижелері overwrite режимінде жазылатындықтан, қайталанған
жұмыс бұрынғы нәтиженің үстіне қайта жазады.

## Деректер мен шығыс

Бастапқы CSV-де 9 980 жол бар. Тазалау name немесе known_for_department
NULL болған жолдарды жояды, бос саланы NULL-ға ауыстырады.
Нәтиже деректеріне gender_label және popularity_tier қосылады.

```text
output/
├── cleaned/
├── reports/
│   ├── dept_stats/
│   ├── gender_distribution/
│   └── top3_by_dept/
├── partitioned/
│   ├── known_for_department=Acting/
│   └── ...
├── taskflow/cleaned/
└── final_report.txt
```

Parquet нәтижесі бір файл емес, Parquet файлдары орналасқан қалта ретінде
сақталады. Жалпы орташа танымалдылық салалардағы орташа мәндерден
адам санына сәйкес салмақпен есептеледі; NULL popularity саны бөлек ескеріледі.
XCom арқылы шағын метадеректер мен жол саны беріледі, DataFrame берілмейді.
Әр Spark callable сессиясын finally арқылы жабады.
TaskFlow нәтижелері негізгі DAG-пен қақтығыспау үшін бөлек қалтаға жазылады.

## GitHub және тапсыру

Осы қалтаның ішіндегі файлдарды жаңа GitHub репозиторийінің түбіріне
жүктеңіз. `.github` қалтасын да қосыңыз. Workflow Ubuntu, Python 3.10,
Java 11 ортасын дайындап, тесттерді және екі толық DAG іске қосылуын тексереді.
Қателер жасырылмайды: толық DAG сәтсіз болса, CI де сәтсіз болады.

Тапсыру: `dags/popular_people_pipeline.py` файлы, GitHub репозиторийінің
сілтемесі және сәтті GitHub Actions нәтижесі.
Өз аккаунтыңыз бен репозиторийіңізге ауыстырып, бейдж қосуға болады:

```markdown
![Airflow](https://github.com/ACCOUNT/REPOSITORY/actions/workflows/check_airflow.yml/badge.svg)
```

Анықтамалар: [Airflow DAG құрылымы](https://airflow.apache.org/docs/apache-airflow/2.8.0/core-concepts/dags.html),
[Airflow орнату және constraints](https://airflow.apache.org/docs/apache-airflow/2.8.1/installation/installing-from-pypi.html).
"# data_engineer_airflow_homework" 
