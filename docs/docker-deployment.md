# Deploying CryptoFlow to EC2 with Docker

This guide covers building the CryptoFlow Docker image on your local Ubuntu workstation, transferring it to an Ubuntu-based Amazon EC2 instance, and running the container there. It assumes you already cloned the repository locally and have SSH access to the EC2 host.

## 1. Local prerequisites

- Ubuntu 20.04+ with Docker Engine installed (`sudo apt install docker.io` or follow the [Docker CE instructions](https://docs.docker.com/engine/install/ubuntu/)).
- Access to the CryptoFlow source tree.
- AWS credentials that permit writes to the target S3 bucket.
- SSH key for your EC2 instance.

## 2. Build the Docker image locally

1. From the repository root, copy the sample environment file and fill in real values:

   ```bash
   cp .env.example .env
   nano .env
   ```

   Supply the S3 credentials (or plan to inject them as environment variables later).

2. Build the image. The provided `Dockerfile` uses a multi-stage build that compiles the Go binary and creates a minimal runtime image:

   ```bash
   docker build -t cryptoflow:latest .
   ```

3. (Optional) Smoke test locally to confirm the container runs:

   ```bash
   docker run --rm --env-file .env \
     -v $(pwd)/config:/app/config \
     --network host \
     cryptoflow:latest -config config/config.yml
   ```

   Using host networking ensures the container can bind to the IPs specified in `config/ip_shards.yml`. When running on EC2 you should do the same.

## 3. Package the image for transfer

If you do not have a container registry available, export the image to a tarball for copying over SSH:

```bash
docker save cryptoflow:latest | gzip > cryptoflow.tar.gz
```

Alternatively, tag and push to a registry that your EC2 instance can reach. The remainder of this guide assumes you copy the tarball.

## 4. Prepare the EC2 instance

1. SSH into the host:

   ```bash
   ssh -i /path/to/key.pem ubuntu@EC2_PUBLIC_IP
   ```

2. Install Docker if it is not already present:

   ```bash
   sudo apt update && sudo apt install -y docker.io
   sudo systemctl enable --now docker
   sudo usermod -aG docker ubuntu  # requires re-login to take effect
   ```

3. Create directories for runtime configuration and logs:

   ```bash
   sudo mkdir -p /opt/cryptoflow/config /var/log/cryptoflow
   sudo chown -R ubuntu:ubuntu /opt/cryptoflow /var/log/cryptoflow
   ```

4. Copy your configuration files to the instance (update the local paths to match your environment):

   ```bash
   scp -i /path/to/key.pem -r config ubuntu@EC2_PUBLIC_IP:/opt/cryptoflow/
   scp -i /path/to/key.pem .env ubuntu@EC2_PUBLIC_IP:/opt/cryptoflow/.env
   ```

   You can maintain separate configs per environment (e.g., `/opt/cryptoflow/config/config.yml` for production).

## 5. Load the image on EC2

1. Upload the image tarball:

   ```bash
   scp -i /path/to/key.pem cryptoflow.tar.gz ubuntu@EC2_PUBLIC_IP:/tmp/
   ```

2. SSH back into the instance and load the image:

   ```bash
   ssh -i /path/to/key.pem ubuntu@EC2_PUBLIC_IP
   cd /tmp
   gunzip cryptoflow.tar.gz
   docker load -i cryptoflow.tar
   docker image ls | grep cryptoflow  # verify it is available
   ```

   Remove the tarball after loading to reclaim space.

## 6. Run the container

Run CryptoFlow using host networking so it can bind to the IP shards configured in `config/ip_shards.yml`:

```bash
cd /opt/cryptoflow
docker run -d --name cryptoflow \
  --env-file /opt/cryptoflow/.env \
  -v /opt/cryptoflow/config:/app/config \
  -v /var/log/cryptoflow:/var/log/cryptoflow \
  --restart unless-stopped \
  --network host \
  cryptoflow:latest -config config/config.yml
```

Key flags:

- `--env-file` injects your AWS credentials without baking them into the image.
- `-v /opt/cryptoflow/config:/app/config` mounts runtime configuration so you can edit it without rebuilding the image.
- `-v /var/log/cryptoflow:/var/log/cryptoflow` persists logs outside the container.
- `--restart unless-stopped` ensures the service starts at boot and restarts on failure.
- `--network host` exposes the container to the host network; required when binding to specific IPs.

Check logs to confirm ingestion:

```bash
docker logs -f cryptoflow
```

Stop and remove the container when needed:

```bash
docker stop cryptoflow && docker rm cryptoflow
```

## 7. Updating the deployment

To roll out a new build:

1. Rebuild the image locally (`docker build -t cryptoflow:latest .`).
2. Export and transfer the new tarball.
3. On EC2, stop the running container, load the new image, and rerun the `docker run` command.

Consider automating these steps with a private container registry or CI/CD pipeline once your GitHub Actions storage issue is resolved.