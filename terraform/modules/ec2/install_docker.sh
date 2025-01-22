#!/bin/bash
# Exit immediately if a command exits with a non-zero status.
set -e

# Update the system
echo "Updating the system..."
sudo apt update -y && sudo apt upgrade -y

# Install required packages for Docker installation
echo "Installing required packages..."
sudo apt install -y apt-transport-https ca-certificates curl software-properties-common

# Add Docker's official GPG key
echo "Adding Docker's official GPG key..."
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -

# Set up the stable repository for Docker
echo "Setting up the Docker repository..."
echo "deb [arch=$(dpkg --print-architecture)] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list

# Update the package index again
echo "Updating the package index again..."
sudo apt update -y

# Install Docker
echo "Installing Docker..."
sudo apt install -y docker-ce

# Start Docker
echo "Starting Docker service..."
sudo systemctl start docker

# Enable Docker to start on boot
echo "Enabling Docker to start on boot..."
sudo systemctl enable docker

# Install Docker Compose
echo "Installing Docker Compose..."
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose

# Make Docker Compose executable
sudo chmod +x /usr/local/bin/docker-compose

# Check Docker Compose version
echo "Checking Docker Compose version..."
docker-compose --version

echo "Docker and Docker Compose installation completed."
