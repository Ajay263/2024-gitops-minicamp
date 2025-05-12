#!/bin/bash
set -e

echo "Updating the system..."
sudo apt update -y && sudo apt upgrade -y

echo "Installing required packages..."
sudo apt install -y apt-transport-https ca-certificates curl software-properties-common

echo "Adding Docker's official GPG key..."
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -

echo "Setting up the Docker repository..."
echo "deb [arch=$(dpkg --print-architecture)] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list

echo "Updating the package index again..."
sudo apt update -y

echo "Installing Docker..."
sudo apt install -y docker-ce

echo "Starting Docker service..."
sudo systemctl start docker

echo "Enabling Docker to start on boot..."
sudo systemctl enable docker

echo "Installing Docker Compose..."
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose

sudo chmod +x /usr/local/bin/docker-compose

echo "Checking Docker Compose version..."
docker-compose --version

echo "Docker and Docker Compose installation completed."
