import subprocess
import settings


def clean_postgres():
    for shard_key in settings.shard_connections.keys():
        label = "pg_" + shard_key
        container_name = label
        data_dir = settings.base_path + "/" + label
        subprocess.run(["docker", "rm", "-f", container_name])
        subprocess.run(["rm", "-rf", data_dir])    


def clean_rabbitmq():
    subprocess.run(["docker", "rm", "-f", "rabbitmq"])


def setup_postgres():
    subprocess.run(["mkdir", "-p", settings.base_path])
    command_template = "docker run -d --name {container_name} -v {data_dir}:/var/lib/postgresql/data -p {port}:5432 -e POSTGRES_PASSWORD=postgres postgres:12"
    for shard_key, conn_params in settings.shard_connections.items():
        label = "pg_" + shard_key
        container_name = label
        data_dir = settings.base_path + "/" + label
        subprocess.run(["mkdir", "-p", data_dir])
        command = command_template.format(
            container_name=container_name,
            data_dir=data_dir,
            port=conn_params.get("port", 5432)
        )
        subprocess.run(command, shell=True)


def setup_rabbitmq():
    command = "docker run -d --name rabbitmq -p 15672:15672 -p 5672:5672 rabbitmq:3-management"
    subprocess.run(command, shell=True)


if __name__ == '__main__':
    # clean_postgres()
    # clean_rabbitmq()
    # setup_postgres()
    # setup_rabbitmq()
    pass