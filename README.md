# Python postgres data copy benchmark

Comparison of batch insert and binary copy.

* For sqlalchemy using psycopg2 (because psycopg3 is not supported yet)
* For binary copy using psycopg3 (because psycopg3 provide better copy protocol implementation that psycopg2)


## Run
```console
$ docker-compose up -d
$ python3 benchmark.py
```

### Extra
* You can modify the benchmark by adding new select queries to the list.
* After each test, the script will wait your input to clear the database and run the next test. You can check the database and make sure that the data is filled in correctly

