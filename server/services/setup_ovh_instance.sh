#!/bin/sh
sudo apt-get update
sudo mkdir -p /home/ubuntu/inftraining_logs
sudo mkdir -p /home/ubuntu/tmp
sudo chown -R $USER /home/ubuntu/inftraining_logs
sudo ln -s /usr/bin/python3 /usr/bin/python
sudo mount /dev/sdb /home/ubuntu
sudo -i
apt install -y apt-transport-https ca-certificates curl gnupg-agent software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable"
apt-get update
apt install docker-ce -y
systemctl enable docker
groupadd docker
usermod -aG docker $USER
chmod 666 /var/run/docker.sock
