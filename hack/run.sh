sudo make fetchit
sudo podman pod stop --all
sudo podman stop --all 
sudo podman pod rm --all --force
sudo podman rm --all --force
sudo podman volume rm --all
sudo podman run -d --rm --name fetchit -v fetchit-volume:/opt -v ~/.fetchit:/opt/mount -v /run/podman/podman.sock:/run/podman/podman.sock quay.io/fetchit/fetchit-amd:latest
