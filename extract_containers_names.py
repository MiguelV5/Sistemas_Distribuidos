import yaml

black_listed_containers = {'rabbitmq', 'server', 'client'}

def get_container_names(docker_compose_file):
    with open(docker_compose_file, 'r') as file:
        compose_data = yaml.safe_load(file)

    services = compose_data.get('services', {})
    container_names = list(services.keys())
    return container_names

def write_config_file(container_names, config_file):
    with open(config_file, 'w') as file:
        for i, name in enumerate(container_names):
            if name in black_listed_containers:
                continue
            if i < len(container_names) - 1:
                file.write(f"{name}\n")
            else:
                file.write(f"{name}")


def main():
    docker_compose_file = 'docker-compose.yaml'
    config_file = 'containers_data.txt'

    container_names = get_container_names(docker_compose_file)
    write_config_file(container_names, config_file)
    print(f"Container names have been written to {config_file}")

if __name__ == "__main__":
    main()