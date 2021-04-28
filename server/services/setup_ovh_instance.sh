#!/bin/sh
sudo -i
apt-get update
mkdir -p /home/ubuntu/model_studio/inftraining_logs
chown -R ubuntu /home/ubuntu/model_studio/inftraining_logs
apt install -y apt-transport-https ca-certificates curl gnupg-agent software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable"
apt install docker-ce -y
systemctl enable docker
chmod 666 /var/run/docker.sock
ln -s /usr/bin/python3 /usr/bin/python
mount /dev/sdb /home/ubuntu/model_studio
