def get_provider_info():
    return {
        "package-name": "apache-airflow-providers-gun",
        "name": "gun",
        "description": "gun",
        "suspended": False,
        "versions": ["0.7.0"],
        "dependencies": [
            "kerberos>=1.3.1",
            "psycopg2>=2.9",
            "clickhouse_driver>=0.2.6",
        ],
        "integrations": [
            {
                "integration-name": "gun",
            }
        ],
        "hooks": [
            {
                "integration-name": "Clickhouse.Native",
                "python-modules": ["airflow.providers.gun.ch.hook"],
            },
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.gun.ch.hook.ClickhouseNativeHook",
                "connection-type": "clickhouse_native",
            }
        ],
    }
