#!/bin/sh
sudo -i
apt-get update
ln -s /usr/bin/python3 /usr/bin/python
mkdir -p /home/ubuntu/model_studio/inftraining_logs
chown -R ubuntu /home/ubuntu/model_studio
apt-get install -y nfs-common
#install docker
apt install -y apt-transport-https ca-certificates curl gnupg-agent software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable"
apt install docker-ce -y
systemctl enable docker
chmod 666 /var/run/docker.sock
