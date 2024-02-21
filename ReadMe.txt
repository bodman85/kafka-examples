podman compose down
podman volume prune

podman compose up -d
podman container exec -it kafka1 bash