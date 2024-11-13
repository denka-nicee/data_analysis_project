from dataclasses import dataclass
from environs import Env


@dataclass
class DbConfig:
    host: str
    password: str
    user: str
    database: str
    port: int = 5432

    def construct_connection_uri(self) -> str:
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    @staticmethod
    def from_env(env: Env):
        host = env.str("POSTGRES_HOST")
        password = env.str("POSTGRES_PASSWORD")
        user = env.str("POSTGRES_USER")
        database = env.str("POSTGRES_DB")
        port = env.int("POSTGRES_PORT", 5432)
        return DbConfig(
            host=host, password=password, user=user, database=database, port=port
        )


@dataclass
class AirflowConfig:
    executor: str
    fernet_key: str

    @staticmethod
    def from_env(env: Env):
        executor = env.str("AIRFLOW__CORE__EXECUTOR", "LocalExecutor")
        fernet_key = env.str("AIRFLOW__CORE__FERNET_KEY")
        return AirflowConfig(executor=executor, fernet_key=fernet_key)


@dataclass
class KaggleConfig:
    username: str
    key: str

    @staticmethod
    def from_env(env: Env):
        username = env.str("KAGGLE_USERNAME", None)
        key = env.str("KAGGLE_KEY", None)
        return KaggleConfig(username=username, key=key)


@dataclass
class Config:
    db: DbConfig
    airflow: AirflowConfig
    kaggle: KaggleConfig


def load_config(path: str = None) -> Config:
    env = Env()
    env.read_env(path)

    return Config(
        db=DbConfig.from_env(env),
        airflow=AirflowConfig.from_env(env),
        kaggle=KaggleConfig.from_env(env),
    )
